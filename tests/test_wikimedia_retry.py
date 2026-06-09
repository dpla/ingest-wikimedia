"""Tests for scripts/wikimedia_retry.py."""

import base64
import re
from unittest.mock import MagicMock, patch


def _decode_staged_script(ssm_cmd: str) -> str:
    """Extract the base64-encoded pipeline script from an SSM staging
    command and return its decoded text. Returns the raw ssm_cmd
    unchanged if no `echo <b64> | base64 -d > /tmp/wm-pipeline-` step
    is present (e.g. a non-launch command like the scan or memory
    check).

    See ingest_wikimedia.ssm.stage_and_launch_tmux for the staging
    wire format. Tests that assert on pipeline *content* (downloader
    args, env exports, awk dedup step) need to decode this — the
    surrounding SSM command only contains the base64 plus the tmux
    wrapper.
    """
    m = re.search(
        r"echo ([A-Za-z0-9+/=]+) \| base64 -d > /tmp/wm-pipeline-",
        ssm_cmd,
    )
    if not m:
        return ssm_cmd
    return base64.b64decode(m.group(1)).decode()


def _find_pipeline_script(commands: list[str]) -> str:
    """Locate the staged-launch SSM command in `commands` and return the
    decoded pipeline script. Asserts the staging step is present so a
    refactor that accidentally drops it surfaces immediately."""
    staged = [c for c in commands if "tmux new-session" in c]
    assert staged, (
        f"no tmux launch command found in captured SSM commands; got: {commands!r}"
    )
    decoded = _decode_staged_script(staged[0])
    assert decoded != staged[0], (
        "expected the launch command to be base64-staged but found no "
        f"`echo <b64> | base64 -d > /tmp/wm-pipeline-` step: {staged[0]!r}"
    )
    return decoded


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
        # stage_and_launch_tmux (in ingest_wikimedia.ssm) calls ssm_run via
        # the module namespace, so the patch on scripts.wikimedia_retry's
        # local import doesn't intercept those. Patch the module-level
        # function too so the staged-launch SSM command lands in `captured`.
        patch("ingest_wikimedia.ssm.ssm_run", side_effect=fake_ssm_run),
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
        # stage_and_launch_tmux (in ingest_wikimedia.ssm) calls ssm_run via
        # the module namespace, so the patch on scripts.wikimedia_retry's
        # local import doesn't intercept those. Patch the module-level
        # function too so the staged-launch SSM command lands in `captured`.
        patch("ingest_wikimedia.ssm.ssm_run", side_effect=fake_ssm_run),
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

    pipeline_cmd = _find_pipeline_script(commands)

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
    pipeline_cmd = _find_pipeline_script(commands)

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


def test_retry_pipeline_uses_raw_dollar_in_notify_fail_cmd():
    r"""The failure handler in the *script* (post base64 decode) must use
    raw `rc=$?` / `WIKIMEDIA_LAST_EXIT=$rc`. The earlier `\$?` / `\$rc`
    form existed only to survive the double-quote layer of an inline
    `tmux new-session "..."` argument; with stage_and_launch_tmux the
    script is base64-decoded into a file and read directly by bash, so
    raw `$?` and `$rc` are evaluated against the actual failing step at
    runtime (which is what we want). A backslash-escaped `\$?` here
    would be interpreted as a literal `$` by bash and `rc` would get the
    string `$?` — defeating the exit-code capture entirely.
    """
    commands = _run_main_and_capture_full_pipeline(
        ["wikimedia_retry.py", "--days", "30", "--partner", "si"]
    )
    pipeline_cmd = _find_pipeline_script(commands)

    assert "rc=$?" in pipeline_cmd, (
        "notify_fail_cmd in the staged script must use raw `rc=$?`; "
        f"got: {pipeline_cmd!r}"
    )
    assert "WIKIMEDIA_LAST_EXIT=$rc" in pipeline_cmd, (
        "notify_fail_cmd must use `WIKIMEDIA_LAST_EXIT=$rc` so the inner "
        f"shell reads the captured exit code; got: {pipeline_cmd!r}"
    )
    # Defence: the backslash-escaped form would now be a bug — `\$?` in
    # a script context reads as literal `$?` (not as the exit code), so
    # if a future refactor brings it back the capture silently breaks.
    assert r"\$?" not in pipeline_cmd, (
        r"found backslash-escaped `\$?` in the staged script; in a script "
        r"file bash reads `\$` as literal `$` so the exit-code capture "
        f"fails. Use raw `$?`. Got: {pipeline_cmd!r}"
    )


def _run_main_with_csvs(argv: list[str], csv_paths: list[str]) -> list[str]:
    """Variant of _run_main_and_capture_full_pipeline that lets the test
    parameterize which retry CSVs the mocked `find` step returns.  The
    fixed helper above is hard-coded to a download-only smithsonian CSV;
    these tests need to exercise the upload-only and both-CSVs paths."""
    captured: list[str] = []

    csv_block = "\n".join(csv_paths)

    def fake_ssm_run(_client, command, **_kwargs):
        captured.append(command)
        if "UPDATE_DONE" in command:
            return "UPDATE_DONE"
        if "get-ids-retry" in command:
            return "Partner found failures.\n"
        if "__MEM_CHECK__" in command:
            return f"{csv_block}\n__MEM_CHECK__\n8000 5000"
        return ""

    with (
        patch("scripts.wikimedia_retry.ssm_run", side_effect=fake_ssm_run),
        # stage_and_launch_tmux (in ingest_wikimedia.ssm) calls ssm_run via
        # the module namespace, so the patch on scripts.wikimedia_retry's
        # local import doesn't intercept those. Patch the module-level
        # function too so the staged-launch SSM command lands in `captured`.
        patch("ingest_wikimedia.ssm.ssm_run", side_effect=fake_ssm_run),
        patch("scripts.wikimedia_retry.boto3.client", return_value=MagicMock()),
        patch("scripts.wikimedia_retry.post_message"),
        patch("sys.argv", argv),
        patch.dict("os.environ", {}, clear=False),
    ):
        from scripts import wikimedia_retry

        try:
            wikimedia_retry.main()
        except SystemExit:
            # Expected: main() reaches _slack_fail after the mocked tmux launch
            # returns "" (no SESSION_STARTED). All SSM commands the test cares
            # about have been captured by this point — mirrors the established
            # pattern in _run_main_and_capture_full_pipeline above.
            pass

    return captured


def test_retry_merges_download_and_upload_csvs_into_single_uploader_call():
    """Regression: when a hub has BOTH a download-retry and upload-retry
    CSV, the pipeline must merge them into ONE combined CSV and run the
    uploader exactly once.

    The pre-fix code ran `uploader <download_csv>` then `uploader <upload_csv>`
    back-to-back, so notify_phase_start("upload") and notify_upload_complete
    both fired TWICE per retry hub. Users saw a confusing "starting upload"
    appear right after the "Wikimedia Retry Complete" summary header.
    """
    commands = _run_main_with_csvs(
        ["wikimedia_retry.py", "--days", "1", "--partner", "nara"],
        [
            "/home/ec2-user/ingest-wikimedia/retry/nara-download-retry.csv",
            "/home/ec2-user/ingest-wikimedia/retry/nara-upload-retry.csv",
        ],
    )
    pipeline_cmd = _find_pipeline_script(commands)

    # The combined CSV merge step must be present and feed the (single)
    # uploader invocation. The `$0` in the awk program is now raw (not
    # backslash-escaped): the staged script is read directly by bash from
    # /tmp/wm-pipeline-<id>.sh, so `$0` reaches awk verbatim and awk
    # evaluates it as the current input line. A backslash-escaped `\$0`
    # would now be the bug — bash would interpret `\$` as literal `$`,
    # passing `awk '!seen[$0]++'` to awk after a quoting layer that
    # already collapsed the backslash.
    expected_combined = (
        "/home/ec2-user/ingest-wikimedia/retry/nara-retry-1d-combined.csv"
    )
    assert (
        "awk '!seen[$0]++' "
        "/home/ec2-user/ingest-wikimedia/retry/nara-download-retry.csv "
        "/home/ec2-user/ingest-wikimedia/retry/nara-upload-retry.csv "
        f"> {expected_combined}"
    ) in pipeline_cmd, (
        f"expected awk merge step missing or malformed; got: {pipeline_cmd!r}"
    )
    # Defence: the backslash-escaped form must NOT appear in the staged
    # script — it'd be interpreted as literal `$` and break the dedup.
    assert r"awk '!seen[\$0]++'" not in pipeline_cmd, (
        r"found backslash-escaped `\$0` in the staged script; bash will "
        r"read `\$` as literal `$` and awk's dedup will be wrong. "
        f"Use raw `$0`. Got: {pipeline_cmd!r}"
    )

    # Exactly ONE `uploader` invocation, pointing at the combined CSV.
    # The downloader still runs separately on the download CSV (that's
    # the actual re-download work), but only one upload pass.
    uploader_calls = [
        seg
        for seg in pipeline_cmd.split("&&")
        if " uploader " in f" {seg.strip()} " or seg.strip().startswith("uploader ")
    ]
    assert len(uploader_calls) == 1, (
        f"expected exactly one uploader invocation in the merged pipeline; "
        f"got {len(uploader_calls)}: {uploader_calls!r}"
    )
    assert expected_combined in uploader_calls[0], (
        f"the single uploader call must target the combined CSV; "
        f"got: {uploader_calls[0]!r}"
    )

    # Sanity: downloader still runs on the download CSV (uploader and
    # downloader work different inputs in this design).
    assert (
        "downloader --max-age-days 1 "
        "/home/ec2-user/ingest-wikimedia/retry/nara-download-retry.csv nara"
    ) in pipeline_cmd


def test_retry_download_only_csv_runs_uploader_once_without_merge_step():
    """Download-only retry path: the existing single-CSV behavior must
    still hold (uploader runs on the download CSV, no awk merge step,
    notify_phase_start + notify_upload_complete fire exactly once)."""
    commands = _run_main_with_csvs(
        ["wikimedia_retry.py", "--days", "30", "--partner", "si"],
        ["/home/ec2-user/ingest-wikimedia/retry/smithsonian-download-retry.csv"],
    )
    pipeline_cmd = _find_pipeline_script(commands)

    assert "awk '!seen" not in pipeline_cmd, (
        f"no merge step expected when only one retry CSV is present; "
        f"got: {pipeline_cmd!r}"
    )
    assert "-retry-30d-combined.csv" not in pipeline_cmd
    # Exactly one uploader invocation, on the download CSV.
    uploader_calls = [
        seg
        for seg in pipeline_cmd.split("&&")
        if " uploader " in f" {seg.strip()} " or seg.strip().startswith("uploader ")
    ]
    assert len(uploader_calls) == 1
    assert (
        "uploader "
        "/home/ec2-user/ingest-wikimedia/retry/smithsonian-download-retry.csv si"
    ) in uploader_calls[0]


def test_retry_upload_only_csv_runs_uploader_once_without_merge_step():
    """Upload-only retry path: same expectation."""
    commands = _run_main_with_csvs(
        ["wikimedia_retry.py", "--days", "30", "--partner", "si"],
        ["/home/ec2-user/ingest-wikimedia/retry/smithsonian-upload-retry.csv"],
    )
    pipeline_cmd = _find_pipeline_script(commands)

    assert "awk '!seen" not in pipeline_cmd
    assert "-retry-30d-combined.csv" not in pipeline_cmd
    # No downloader call — there's nothing to re-download for an upload-
    # only retry; the items were already in S3 from their original run.
    assert "downloader" not in pipeline_cmd, (
        f"upload-only retry must not invoke downloader; got: {pipeline_cmd!r}"
    )
    uploader_calls = [
        seg
        for seg in pipeline_cmd.split("&&")
        if " uploader " in f" {seg.strip()} " or seg.strip().startswith("uploader ")
    ]
    assert len(uploader_calls) == 1
    assert (
        "uploader /home/ec2-user/ingest-wikimedia/retry/smithsonian-upload-retry.csv si"
    ) in uploader_calls[0]


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


def test_retry_sdc_only_csv_runs_sdc_sync_step():
    """When only an sdc-retry CSV exists for a hub, the pipeline runs
    sdc-sync (partner mode against that CSV) and does NOT invoke the
    downloader or uploader — S3 assets and Commons pages are already
    correct; only SDC needs to be re-attempted."""
    commands = _run_main_with_csvs(
        ["wikimedia_retry.py", "--days", "7", "--partner", "nara"],
        ["/home/ec2-user/ingest-wikimedia/retry/nara-sdc-retry.csv"],
    )
    pipeline_cmd = _find_pipeline_script(commands)

    assert "downloader" not in pipeline_cmd, (
        f"sdc-only retry must not invoke downloader; got: {pipeline_cmd!r}"
    )
    assert "uploader" not in pipeline_cmd, (
        f"sdc-only retry must not invoke uploader; got: {pipeline_cmd!r}"
    )
    assert (
        "sdc-sync --partner nara --ids-file /home/ec2-user/ingest-wikimedia/retry/nara-sdc-retry.csv"
        in pipeline_cmd
    )


def test_retry_sdc_runs_after_upload_when_both_csvs_present():
    """Upload + SDC retries on the same hub: the sdc-sync step runs
    AFTER the uploader because the uploader refreshes upload-result.json
    on S3, which sdc-sync reads to find SDC-eligible ordinals."""
    commands = _run_main_with_csvs(
        ["wikimedia_retry.py", "--days", "7", "--partner", "nara"],
        [
            "/home/ec2-user/ingest-wikimedia/retry/nara-upload-retry.csv",
            "/home/ec2-user/ingest-wikimedia/retry/nara-sdc-retry.csv",
        ],
    )
    pipeline_cmd = _find_pipeline_script(commands)

    upload_pos = pipeline_cmd.find("uploader ")
    sdc_pos = pipeline_cmd.find("sdc-sync ")
    assert upload_pos != -1, f"uploader step missing: {pipeline_cmd!r}"
    assert sdc_pos != -1, f"sdc-sync step missing: {pipeline_cmd!r}"
    assert upload_pos < sdc_pos, (
        f"sdc-sync must follow uploader (uploader writes upload-result.json "
        f"that sdc-sync reads); got positions upload={upload_pos} sdc={sdc_pos}"
    )
