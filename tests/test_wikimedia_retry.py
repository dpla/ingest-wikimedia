"""Tests for scripts/wikimedia_retry.py."""

from unittest.mock import MagicMock, patch


def _run_main_and_capture_ssm_commands(argv: list[str]) -> list[str]:
    """Run wikimedia_retry.main() with all external side-effects mocked, and
    return the list of SSM command strings it tried to run.

    The script ssm_runs three commands before launching the tmux pipeline:
      1. EC2 code update (must echo `UPDATE_DONE` for the script to proceed)
      2. The scan command (the one this PR is about — must clear stale
         retry CSVs before invoking get-ids-retry)
      3. The find + memory-check combined call

    We short-circuit at step 3 by returning "No retryable failures found"
    from step 2, which makes main() exit cleanly without trying to build
    the tmux pipeline. This keeps the test focused and avoids having to
    mock out the entire post-scan flow.
    """
    captured: list[str] = []

    def fake_ssm_run(_client, command, **_kwargs):
        captured.append(command)
        # First call is update_cmd; must include "UPDATE_DONE" so main proceeds.
        if "UPDATE_DONE" in command:
            return "UPDATE_DONE"
        # Second call is the scan; pretend it found nothing so main exits.
        if "get-ids-retry" in command:
            return "No retryable failures found in the last 30 days."
        return ""

    with (
        patch("scripts.wikimedia_retry.ssm_run", side_effect=fake_ssm_run),
        patch("scripts.wikimedia_retry.boto3.client", return_value=MagicMock()),
        patch("sys.argv", argv),
        patch.dict("os.environ", {}, clear=False),
    ):
        from scripts import wikimedia_retry

        try:
            wikimedia_retry.main()
        except SystemExit:
            pass  # script exits cleanly on "no retryable failures"

    return captured


def test_retry_clears_stale_csvs_before_scan():
    """Regression: a `/wikimedia-upload retry 30 si` call must not pick up
    stale CSVs that previous (unrelated) retry runs left in
    /home/ec2-user/ingest-wikimedia/retry/.

    Before this fix, the scan only wrote `smithsonian-download-retry.csv`,
    but `find {retry_dir} -name '*-retry.csv'` also matched leftover
    `indiana-…`, `nara-…`, `ohio-…` CSVs from runs days or weeks earlier.
    The tmux pipeline then launched retry blocks for every one of those
    hubs, ignoring the user's explicit `si` scope entirely.

    Assert that the SSM command which runs `get-ids-retry` also clears
    any pre-existing `*-retry.csv` first, so `find` later sees only the
    current scan's output.
    """
    commands = _run_main_and_capture_ssm_commands(
        ["wikimedia_retry.py", "--days", "30", "--partner", "si"]
    )

    scan_cmds = [c for c in commands if "get-ids-retry" in c]
    assert scan_cmds, f"No scan command was issued; saw: {commands!r}"
    scan_cmd = scan_cmds[0]

    rm_idx = scan_cmd.find("rm -f")
    scan_idx = scan_cmd.find("get-ids-retry")
    assert rm_idx >= 0, (
        f"scan command is missing the stale-CSV clearing step: {scan_cmd!r}"
    )
    assert rm_idx < scan_idx, (
        f"rm must come BEFORE get-ids-retry, otherwise it would delete the "
        f"freshly-written CSVs: {scan_cmd!r}"
    )
    # Make sure the rm pattern is scoped to *-retry.csv only; deleting the
    # whole directory contents would clobber unrelated files.
    assert "*-retry.csv" in scan_cmd, (
        f"rm pattern must be limited to *-retry.csv to avoid clobbering "
        f"unrelated files in the retry dir: {scan_cmd!r}"
    )


def _run_main_and_capture_full_pipeline(argv: list[str]) -> list[str]:
    """Like `_run_main_and_capture_ssm_commands` but lets main() proceed
    past the scan step so the final tmux pipeline launch command is
    captured. Returns the full list of SSM command strings issued in order.

    Mock plan, in the order main() issues calls:
      1. update_cmd                      → return "UPDATE_DONE"
      2. scan_cmd (get-ids-retry)        → return a "found 1 failure" line
      3. find + memory check             → return a CSV path + ample memory
      4. tmux ls (conflict detection)    → return empty (no conflicts)
      5. tmux new-session …              → captured for assertion
    Any subsequent calls (Slack notification, etc.) return "" — they don't
    matter for the assertions in this file.
    """
    captured: list[str] = []

    def fake_ssm_run(_client, command, **_kwargs):
        captured.append(command)
        if "UPDATE_DONE" in command:
            return "UPDATE_DONE"
        if "get-ids-retry" in command:
            # Pretend the scan found one failure so main keeps going.
            return (
                "Partner       Type    IDs  File\n"
                "----------------------------------\n"
                "smithsonian   download   1  /home/ec2-user/ingest-wikimedia/retry/smithsonian-download-retry.csv\n"
            )
        if "__MEM_CHECK__" in command:
            return (
                "/home/ec2-user/ingest-wikimedia/retry/smithsonian-download-retry.csv\n"
                "__MEM_CHECK__\n"
                "8000 5000"
            )
        # tmux ls and any later calls
        return ""

    with (
        patch("scripts.wikimedia_retry.ssm_run", side_effect=fake_ssm_run),
        patch("scripts.wikimedia_retry.boto3.client", return_value=MagicMock()),
        patch("scripts.wikimedia_retry.post_message"),  # no real Slack
        patch("sys.argv", argv),
        patch.dict("os.environ", {}, clear=False),
    ):
        from scripts import wikimedia_retry

        try:
            wikimedia_retry.main()
        except SystemExit:
            # main() reaches `_slack_fail` after the mocked tmux launch
            # returns "" (no SESSION_STARTED), which calls sys.exit(1).
            # That's expected in the mocked path — by that point we've
            # already captured every SSM call the test cares about.
            pass

    return captured


def test_retry_passes_max_age_days_one_to_downloader():
    """Regression: the retry-driven downloader call must pass
    `--max-age-days 1` instead of falling back to the CLI default of 365.

    Without this flag, a refresh-failure retry would re-attempt the failed
    file, see its S3 LastModified is N < 365 days old, log
    "Key already in S3", and silently SKIP — exactly the file the user
    was trying to retry. With --max-age-days 1, successful refreshes
    (S3 age ~0 days) still skip correctly while refresh-failed files
    (S3 age >= 1 day) actually re-attempt.

    Scenario the bug bites:
      * /wikimedia-upload refresh si 1 — file 4 of item X is 58 days
        old in S3; refresh decides to refresh; source request fails
        transiently; file 4's S3 LastModified stays at 58 days old.
      * /wikimedia-upload retry 30 si — retry CSV includes item X;
        downloader runs over item X's files; file 4's age (58) < default
        365 → "Key already in S3" → SKIP. The retry user expected the
        failed file to be re-attempted, but it was silently dropped.
    """
    commands = _run_main_and_capture_full_pipeline(
        ["wikimedia_retry.py", "--days", "30", "--partner", "si"]
    )

    pipeline_cmds = [c for c in commands if "tmux new-session" in c]
    assert pipeline_cmds, f"No tmux new-session command was issued; saw: {commands!r}"
    pipeline_cmd = pipeline_cmds[0]

    assert "downloader --max-age-days 1" in pipeline_cmd, (
        f"The retry's downloader call must pass `--max-age-days 1` so "
        f"refresh-failed files actually re-attempt; got: {pipeline_cmd!r}"
    )
    # Defence: make sure --max-age-days 1 is immediately followed by the
    # CSV path and partner argument, not buried elsewhere in the command
    # line. The substring match here pins the exact argument order the
    # downloader CLI expects (`downloader [OPTIONS] IDS_FILE PARTNER`).
    expected = (
        "downloader --max-age-days 1 "
        "/home/ec2-user/ingest-wikimedia/retry/smithsonian-download-retry.csv si"
    )
    assert expected in pipeline_cmd, (
        f"expected substring not found; got: {pipeline_cmd!r}"
    )


def test_retry_pipeline_does_not_export_download_log_via_pwd_subshell():
    """Regression guard: the pipeline must NOT try to capture the download
    log via `export WIKIMEDIA_RETRY_DOWNLOAD_LOG="$PWD/$(ls -t ...)"`.

    That form was the original implementation of the combined-summary
    feature and it looked correct — but the outer bash that constructs
    the tmux session argument eagerly expanded `$PWD` (the SSM agent's
    cwd, typically `/usr/bin/`) before the inner shell ever ran
    `cd {partner_dir}`. The exported value was `/usr/bin/` on every
    real run, and notify_upload_complete dutifully tried to read it as a
    log file, failed, and silently degraded to upload-only "0 failed"
    summaries even when downloads had bombed.

    The replacement design: notify_upload_complete discovers its own
    download log from WIKIMEDIA_SESSION_LABEL + WIKIMEDIA_PARTNER_DIR
    (both exported as literal shlex.quote-d values that survive the
    outer-bash parse intact). See slack._find_retry_download_log.
    """
    commands = _run_main_and_capture_full_pipeline(
        ["wikimedia_retry.py", "--days", "30", "--partner", "si"]
    )
    pipeline_cmd = next(c for c in commands if "tmux new-session" in c)

    assert "WIKIMEDIA_RETRY_DOWNLOAD_LOG" not in pipeline_cmd, (
        "pipeline must not export WIKIMEDIA_RETRY_DOWNLOAD_LOG — the "
        "$PWD-based form is a known footgun (outer-bash expansion timing). "
        f"got: {pipeline_cmd!r}"
    )
    # The label + partner-dir exports that the new discovery flow depends
    # on must still be present and must precede the uploader.
    assert "export WIKIMEDIA_SESSION_LABEL=" in pipeline_cmd
    assert "export WIKIMEDIA_PARTNER_DIR=" in pipeline_cmd
    # WIKIMEDIA_RETRY_HAS_DOWNLOAD must be set to 1 when a download CSV is
    # present, so notify_upload_complete knows the discovered download log
    # belongs to this run rather than a prior retry session that reused
    # the same label.
    assert "export WIKIMEDIA_RETRY_HAS_DOWNLOAD=1" in pipeline_cmd, (
        "pipeline must opt into combined-summary log discovery via "
        "WIKIMEDIA_RETRY_HAS_DOWNLOAD=1 when the target has a download "
        f"phase; got: {pipeline_cmd!r}"
    )


def test_retry_clears_stale_csvs_even_when_no_partner_given():
    """When the user runs `/wikimedia-upload retry 30` (no partner), the
    scan still needs to clear stale CSVs so that hubs which legitimately
    had no transient failures in the current window aren't re-launched
    from leftover artifacts of a prior scan."""
    commands = _run_main_and_capture_ssm_commands(
        ["wikimedia_retry.py", "--days", "30"]
    )

    scan_cmds = [c for c in commands if "get-ids-retry" in c]
    assert scan_cmds
    scan_cmd = scan_cmds[0]
    assert "rm -f" in scan_cmd
    assert "*-retry.csv" in scan_cmd
    assert scan_cmd.find("rm -f") < scan_cmd.find("get-ids-retry")
