"""Tests for scripts/wikimedia_upload_status.py helpers."""

import re


def test_log_filename_pattern_matches_only_exact_label():
    """Sibling labels that extend the search label must NOT match.

    Regression test for the status-stuck-on-wrong-target bug: when a chained
    pipeline runs both `bpl+phillips-academy` and `bpl+phillips-academy-andover`,
    the status fetcher must not pick up the andover log when checking on
    bpl+phillips-academy (or vice versa). A bare substring match misclassified
    these and reported the wrong target's log file.
    """
    from scripts.wikimedia_upload_status import log_filename_pattern_for_label

    pattern = re.compile(log_filename_pattern_for_label("bpl+phillips-academy"))

    # Exact-label matches
    assert pattern.search("20260522-203316-bpl+phillips-academy-download.log")
    assert pattern.search("20260522-203316-bpl+phillips-academy-upload.log")

    # Sibling labels whose names extend "bpl+phillips-academy" must NOT match
    assert not pattern.search(
        "20260523-065246-bpl+phillips-academy-andover-download.log"
    )
    assert not pattern.search("20260523-065248-bpl+phillips-academy-andover-upload.log")

    # Legacy hub-only logs must NOT match (different format, handled separately)
    assert not pattern.search("20260513-211920-bpl-download.log")
    assert not pattern.search("20260513-211920-bpl-upload.log")

    # Unrelated hub must NOT match
    assert not pattern.search("20260522-100000-ia+phillips-academy-download.log")


def test_log_filename_pattern_matches_only_phase_suffixes():
    """Only -download.log, -upload.log, and -sdc.log are valid phase logs."""
    from scripts.wikimedia_upload_status import log_filename_pattern_for_label

    pattern = re.compile(log_filename_pattern_for_label("nara"))
    assert pattern.search("20260522-100000-nara-download.log")
    assert pattern.search("20260522-100000-nara-upload.log")
    assert pattern.search("20260522-100000-nara-sdc.log")
    # Other phases (e.g. legacy retirer logs) must NOT match
    assert not pattern.search("20251220-012010-nara-retirer.log")
    assert not pattern.search("20260522-100000-nara-fix.log")


def test_log_filename_pattern_handles_regex_metachars_in_label():
    """Labels contain `+` which is a regex metacharacter — must be escaped."""
    from scripts.wikimedia_upload_status import log_filename_pattern_for_label

    pattern = re.compile(log_filename_pattern_for_label("indiana+benjamin-harrison"))
    # The literal label should match
    assert pattern.search("20260522-100000-indiana+benjamin-harrison-download.log")
    # The `+` must NOT be treated as a regex quantifier ("indianabenjamin..." should fail)
    assert not pattern.search("20260522-100000-indianabenjamin-harrison-download.log")


def test_get_phase_and_progress_grep_uses_double_dash_separator():
    """Regression: get_phase_and_progress must pass `grep -E -- {pattern}`,
    not `grep -E {pattern}`, because the pattern starts with `-` (see the
    test above). Without `--`, grep emits "invalid option" and the status
    reporter silently misclassifies every label.
    """
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    captured_commands: list[str] = []

    def fake_ssm_run(_client, command, **_kwargs):
        captured_commands.append(command)
        # First call is the precheck (session_created + ls|grep); subsequent
        # calls only happen if a log file was returned. Return an empty
        # session_created and no log file so we exit early.
        if len(captured_commands) == 1:
            return "0\n"
        return ""

    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake_ssm_run):
        get_phase_and_progress(
            client=None,
            session="wikimedia-bpl+phillips-academy",
            hub="bpl",
            label="bpl+phillips-academy",
        )

    assert captured_commands, "get_phase_and_progress should have invoked ssm_run"
    precheck = captured_commands[0]
    assert "grep -E -- " in precheck, (
        f"precheck must use `grep -E --` to keep grep from treating the "
        f"leading-`-` pattern as an option; got: {precheck!r}"
    )


def test_log_filename_pattern_always_starts_with_dash():
    """Regression: any pattern this builder returns begins with `-`, which
    means callers invoking grep with it MUST use `--` (or `-e <pattern>`)
    to keep grep from interpreting the leading `-` as a flag.

    Without that terminator, grep emits `invalid option -- 'X'` for whatever
    follows the dash and exits non-zero — silently returning no matches.
    `head -1` then returns empty, get_phase_and_progress returns None for
    every label, and the status reporter falls back to "Generating IDs" on
    the first pending label of every multi-target session.
    """
    from scripts.wikimedia_upload_status import log_filename_pattern_for_label

    for label in (
        "bpl",
        "bpl+phillips-academy",
        "indiana+benjamin-harrison",
        "nara",
        "retry-indiana",
    ):
        assert log_filename_pattern_for_label(label).startswith("-"), (
            f"Pattern for {label!r} must lead with `-` so the anchor before "
            "the label binds; callers must compensate with `grep -E --`."
        )


def _fake_ssm_for_phase(log_filename: str, awk_counts: list[int], csv_total: int):
    """Build a fake `ssm_run` that returns the precheck + counts payload
    `get_phase_and_progress` expects, with the named log file and counts.

    Sequence (matches the real two-call flow):
      1. precheck — `session_created\nlog_filename`
      2. main — `now\nmtime\nSEP\ntail\nSEP\n<5 lines of awk + wc>`
    """
    call_count = [0]
    sep = "__WM_SEP__"

    def fake_ssm_run(_client, _command, **_kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return f"1700000000\n{log_filename}\n"
        # now=mtime so no staleness suffix; counts payload then csv_total
        body = f"1700000000\n1700000000\n{sep}\nlast log line\n{sep}\n"
        body += "\n".join(str(n) for n in awk_counts) + "\n"
        body += f"{csv_total}\n"
        return body

    return fake_ssm_run


def test_get_phase_and_progress_reports_sdc_syncing_in_progress():
    """An -sdc.log with DPLA IDs logged but no terminal COUNTS marker
    surfaces as "SDC syncing (N / total items, ~pct%)".
    """
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    # awk_counts layout: [dpla_id_count, uploaded, skipping, counts_marker]
    fake = _fake_ssm_for_phase(
        log_filename="20260525-200000-minnesota-sdc.log",
        awk_counts=[3, 0, 0, 0],
        csv_total=10,
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        phase = get_phase_and_progress(
            client=None,
            session="wikimedia-minnesota",
            hub="minnesota",
            label="minnesota",
        )
    assert phase.startswith("SDC syncing")
    assert "3" in phase
    assert "10" in phase


def test_get_phase_and_progress_reports_sdc_complete():
    """An -sdc.log whose awk pass found a COUNTS: terminal marker surfaces
    as "SDC complete (N items synced)" so the walker can mark it done.
    """
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    fake = _fake_ssm_for_phase(
        log_filename="20260525-200000-minnesota-sdc.log",
        awk_counts=[10, 0, 0, 1],  # 10 items, COUNTS marker present
        csv_total=10,
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        phase = get_phase_and_progress(
            client=None,
            session="wikimedia-minnesota",
            hub="minnesota",
            label="minnesota",
        )
    assert phase.startswith("SDC complete"), phase
    assert "10" in phase


def test_get_phase_and_progress_reports_sdc_starting_with_no_items():
    """An -sdc.log that exists but hasn't logged any `DPLA ID:` lines yet
    surfaces as "SDC syncing (starting...)" — no false staleness."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    fake = _fake_ssm_for_phase(
        log_filename="20260525-200000-minnesota-sdc.log",
        awk_counts=[0, 0, 0, 0],
        csv_total=10,
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        phase = get_phase_and_progress(
            client=None,
            session="wikimedia-minnesota",
            hub="minnesota",
            label="minnesota",
        )
    assert phase == "SDC syncing (starting...)"
