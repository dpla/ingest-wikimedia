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
