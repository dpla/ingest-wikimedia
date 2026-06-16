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


def _fake_ssm_for_phase(
    log_filename: str,
    awk_counts: list[int],
    csv_total: int,
    *,
    mtime: int = 1700000000,
    now: int | None = None,
    tail: str = "last log line",
):
    """Build a fake `ssm_run` that returns the precheck + counts payload
    `get_phase_and_progress` expects, with the named log file and counts.

    Sequence (matches the real two-call flow):
      1. precheck — `session_created\nlog_filename`
      2. main — `now\nmtime\nSEP\ntail\nSEP\n<5 lines of awk + wc>`

    ``mtime`` is parameterised so tests can verify the mtime tiebreak in
    ``main`` — an "aborted phase" fake with an earlier mtime must be
    distinguishable from an "active phase" fake with a later mtime, even
    though both produce a non-COUNTS-marker phase string.  ``now`` is
    kept at the same value so the staleness suffix doesn't fire (it
    would change the phase-string assertions in unrelated callers).
    """
    call_count = [0]
    sep = "__WM_SEP__"

    now_val = mtime if now is None else now

    def fake_ssm_run(_client, _command, **_kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return f"{mtime}\n{log_filename}\n"
        # Default now == mtime so the staleness suffix never fires (idle == 0);
        # callers pass an explicit later ``now`` to exercise staleness.
        body = f"{now_val}\n{mtime}\n{sep}\n{tail}\n{sep}\n"
        body += "\n".join(str(n) for n in awk_counts) + "\n"
        body += f"{csv_total}\n"
        return body

    return fake_ssm_run


def test_get_phase_and_progress_retry_label_reads_retry_dir_csvs():
    """Regression: for `retry-<slug>` labels the CSV "items in scope"
    denominator must come from the retry pipeline's CSV(s) in the shared
    /retry/ directory, NOT from `{partner_base}/retry-<slug>.csv` (which
    never exists). Without this, every retry session's status reported
    "N / 0 items" because the wc -l fell back to the `|| echo 0` branch.

    `northwest-heritage` is a partner whose EC2 directory name matches its
    canonical slug (so PARTNER_DIR has no override), exercising the
    pdir=hub fall-through.
    """
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    captured: list[str] = []

    def fake_ssm_run(_client, command, **_kwargs):
        captured.append(command)
        if len(captured) == 1:
            return "1700000000\n20260601-120000-retry-northwest-heritage-download.log\n"
        # awk pass: dpla_id=2, uploaded=0, skipping=0, counts=0;
        # then csv total = 5 (sum of download+upload retry CSVs)
        return (
            "1700000000\n1700000000\n__WM_SEP__\nDownloading something\n"
            "__WM_SEP__\n2\n0\n0\n0\n5\n"
        )

    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake_ssm_run):
        phase, _ = get_phase_and_progress(
            client=None,
            session="wikimedia-retry-7d-northwest-heritage",
            hub="northwest-heritage",
            label="retry-northwest-heritage",
        )
    assert phase is not None
    # Phase is "Downloading (2 / 5 items, ~40.0%)" — total must be 5, not 0.
    assert "2" in phase and "5" in phase, phase
    assert "/ 0 items" not in phase, (
        f"Status should not show '/ 0 items' for retry sessions; got: {phase!r}"
    )

    # The main SSM call must reference the retry/-directory CSVs, not the
    # nonexistent {base}/retry-{slug}.csv. Pin both paths so neither half
    # of the lookup can regress.
    main_cmd = captured[1]
    assert "/retry/northwest-heritage-download-retry.csv" in main_cmd, main_cmd
    assert "/retry/northwest-heritage-upload-retry.csv" in main_cmd, main_cmd
    # The bogus path the bug was reading must NOT be in the command.
    assert "/retry-northwest-heritage.csv" not in main_cmd, (
        f"status must not look for `{{base}}/retry-<slug>.csv` for retry "
        f"sessions; got: {main_cmd!r}"
    )


def test_get_phase_and_progress_retry_label_uses_partner_dir_name():
    """Retry CSVs are named by partner *directory*, not canonical slug.
    For `si` (canonical slug) the directory is `smithsonian`, so the CSV
    is `/retry/smithsonian-download-retry.csv`, not
    `/retry/si-download-retry.csv`. The status script must honor
    PARTNER_DIR the same way the retry pipeline does.
    """
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    captured: list[str] = []

    def fake_ssm_run(_client, command, **_kwargs):
        captured.append(command)
        if len(captured) == 1:
            return "1700000000\n20260601-120000-retry-si-download.log\n"
        return "1700000000\n1700000000\n__WM_SEP__\n.\n__WM_SEP__\n1\n0\n0\n0\n3\n"

    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake_ssm_run):
        get_phase_and_progress(
            client=None,
            session="wikimedia-retry-7d-si",
            hub="si",
            label="retry-si",
        )
    main_cmd = captured[1]
    assert "/retry/smithsonian-download-retry.csv" in main_cmd
    assert "/retry/smithsonian-upload-retry.csv" in main_cmd
    assert "/retry/si-" not in main_cmd, (
        f"must use partner dir name (smithsonian), not slug (si); got: {main_cmd!r}"
    )


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
        phase, log_mtime = get_phase_and_progress(
            client=None,
            session="wikimedia-minnesota",
            hub="minnesota",
            label="minnesota",
        )
    assert phase.startswith("SDC syncing")
    assert "3" in phase
    assert "10" in phase
    # mtime is the second tuple element — present so callers can compare
    # across labels (see test_main_picks_latest_active_label_by_mtime).
    assert log_mtime > 0


def test_get_phase_and_progress_flags_waiting_on_slots():
    """When the sdc-log tail's last line is the slot-budget wait message,
    the phase is annotated 'waiting on slots' — and the idle/stale warning
    is suppressed (a blocked session legitimately stops writing its log)."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    fake = _fake_ssm_for_phase(
        log_filename="20260616-000000-nara+x-sdc.log",
        awk_counts=[100, 0, 0, 0],
        csv_total=200,
        mtime=1700000000,
        now=1700000000
        + 3600,  # 1h since last log write — would be "idle" if not waiting
        tail=" -- All 16 worker slots busy; waiting for capacity.",
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        phase, _ = get_phase_and_progress(
            client=None, session="wikimedia-nara+x", hub="nara", label="nara+x"
        )
    assert phase.startswith("SDC syncing")
    assert "waiting on slots" in phase
    assert "idle" not in phase, "stale/idle warning must be suppressed while waiting"


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
        phase, _ = get_phase_and_progress(
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
        phase, _ = get_phase_and_progress(
            client=None,
            session="wikimedia-minnesota",
            hub="minnesota",
            label="minnesota",
        )
    assert phase == "SDC syncing (starting...)"


def test_main_picks_latest_active_label_by_mtime_when_earlier_label_aborted():
    """Regression: when a multi-target session has an EARLIER label whose
    phase ended without a COUNTS terminal marker (e.g. an SDC sync that
    aborted via SystemExit) and a LATER label is actively progressing
    right now, the reporter must surface the LATER one, not freeze on
    the stale earlier one.

    Concrete case (May 28 2026): a six-target NARA pipeline aborted the
    Clinton SDC at item 250/4879 around 09:57 UTC.  The shell-level
    target separator is `;` (not `&&`), so subsequent targets ran:
    Eisenhower SDC completed, Presidential Materials Division SDC
    completed, and Center for Legislative Archives SDC is currently
    running.  The reporter was reporting `[nara+william-j-clinton-library]
    SDC syncing (250 / 4,879 items, ~5.1%) ⚠ idle 5h38m` because its
    forward-walk returned the first non-complete phase it found.
    """
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    # The fix swaps the walk for an mtime-based tiebreak.  Verify the
    # primitive that backs it: get_phase_and_progress reports the
    # log_mtime alongside the phase, and an aborted older phase reports
    # a smaller mtime than a currently-active later phase.  We then run
    # the same `max(..., key=lambda t: t[2])` selection main() uses, to
    # demonstrate that the active phase wins.
    ABORT_MTIME = 1700000000  # the moment Clinton SDC aborted
    ACTIVE_MTIME = ABORT_MTIME + 5 * 3600  # ~5h later: CFLA SDC last write

    aborted = _fake_ssm_for_phase(
        log_filename="20260528-091047-nara+william-j-clinton-library-sdc.log",
        awk_counts=[250, 0, 0, 0],  # no COUNTS marker → not complete
        csv_total=4879,
        mtime=ABORT_MTIME,
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=aborted):
        aborted_phase, aborted_mtime = get_phase_and_progress(
            client=None,
            session="wikimedia-multi",
            hub="nara",
            label="nara+william-j-clinton-library",
        )

    active = _fake_ssm_for_phase(
        log_filename="20260528-140954-nara+center-for-legislative-archives-sdc.log",
        awk_counts=[3101, 0, 0, 0],
        csv_total=6946,
        mtime=ACTIVE_MTIME,
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=active):
        active_phase, active_mtime = get_phase_and_progress(
            client=None,
            session="wikimedia-multi",
            hub="nara",
            label="nara+center-for-legislative-archives",
        )

    # Both phases say "SDC syncing" (neither has COUNTS) — both look
    # "active" by the count-marker test, which is exactly what tripped
    # up the pre-fix forward-walk: it stopped at the first non-complete
    # label and never advanced.
    assert aborted_phase.startswith("SDC syncing")
    assert active_phase.startswith("SDC syncing")
    assert aborted_mtime > 0 and active_mtime > 0

    # The contract that backs the tiebreak: distinct mtimes are returned
    # and the active one is strictly later than the aborted one.
    assert active_mtime > aborted_mtime
    assert active_mtime - aborted_mtime == 5 * 3600

    # Run the same selection main() runs on the (label, phase, mtime)
    # tuples: pick the latest-mtime active label.  The active label
    # must win — otherwise we're back to the "stuck on Clinton" bug.
    active_labels = [
        ("nara+william-j-clinton-library", aborted_phase, aborted_mtime),
        ("nara+center-for-legislative-archives", active_phase, active_mtime),
    ]
    chosen_label, chosen_phase, _ = max(active_labels, key=lambda t: t[2])
    assert chosen_label == "nara+center-for-legislative-archives"
    assert chosen_phase == active_phase


def test_get_phase_and_progress_returns_none_tuple_when_no_log_exists():
    """The signature change must preserve the `phase is None` sentinel:
    when no log file matches, return ``(None, 0)`` so the caller's
    membership-and-tiebreak loop can detect "no log" and skip the
    label without crashing on a string operation."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    # Return empty for both ls invocations (no matching log + no hub
    # fallback log) so the function takes the "no log" early-return path.
    def fake_ssm_run(_client, _command, **_kwargs):
        return "1700000000\n\n"

    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake_ssm_run):
        result = get_phase_and_progress(
            client=None,
            session="wikimedia-unknown",
            hub="bpl",
            label="bpl+nonexistent",
        )
    assert result == (None, 0)


def test_format_memory_line_formats_pair():
    """``_format_memory_line`` turns the raw ``(total, available)``
    pair from ``ingest_wikimedia.ssm.fetch_memory_snapshot`` into the
    Slack-friendly ``"used / total MB used (pct% available)"`` line.
    The OOM-watching readout has to be parseable at a glance — pin
    the exact format so a future thousands-separator or label tweak
    is noticed."""
    from scripts.wikimedia_upload_status import _format_memory_line

    # used = total - available = 7700 - 3200 = 4500
    # pct_available = 3200 * 100 // 7700 = 41
    assert (
        _format_memory_line((7700, 3200))
        == "Memory: 4,500 / 7,700 MB used (41% available)"
    )


def test_format_memory_line_passes_through_none():
    """When the shared helper couldn't fetch a snapshot it returns
    ``None``; the formatter has to propagate that so the caller can
    omit the memory block from the Slack post rather than emit a
    misleading row."""
    from scripts.wikimedia_upload_status import _format_memory_line

    assert _format_memory_line(None) is None


def test_post_to_slack_includes_memory_line_when_provided():
    """``post_to_slack`` adds the memory snapshot as a context block at
    the bottom of the message. Omitted when ``memory_line`` is None
    (so the message degrades gracefully on snapshot failure)."""
    from unittest.mock import MagicMock, patch

    from scripts.wikimedia_upload_status import post_to_slack

    fake_resp = MagicMock()
    fake_resp.json.return_value = {"ok": True}
    fake_resp.raise_for_status.return_value = None

    rows = [("wikimedia-bpl", "Uploading")]
    with patch(
        "scripts.wikimedia_upload_status.requests.post", return_value=fake_resp
    ) as mock_post:
        post_to_slack(
            "tok", rows, memory_line="Memory: 4,500 / 7,700 MB used (41% available)"
        )
    payload = mock_post.call_args.kwargs["json"]
    block_texts = [
        elem["text"]
        for block in payload["blocks"]
        if block["type"] == "context"
        for elem in block["elements"]
    ]
    assert any("41% available" in text for text in block_texts)


def test_post_to_slack_omits_memory_block_when_none():
    """No memory line → no context block — the message degrades to just
    the session phase summary."""
    from unittest.mock import MagicMock, patch

    from scripts.wikimedia_upload_status import post_to_slack

    fake_resp = MagicMock()
    fake_resp.json.return_value = {"ok": True}
    fake_resp.raise_for_status.return_value = None

    with patch(
        "scripts.wikimedia_upload_status.requests.post", return_value=fake_resp
    ) as mock_post:
        post_to_slack("tok", [("wikimedia-bpl", "Uploading")], memory_line=None)
    payload = mock_post.call_args.kwargs["json"]
    assert not any(b.get("type") == "context" for b in payload["blocks"])


def test_format_slots_line_reports_free_headroom():
    """Median of the held-count samples → free = total − held; reports the
    stable headroom count, not who holds what."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import _format_slots_line

    # TOTAL 24, held samples [16,14,17,15] → median 15.5 → 16 held → 8 free.
    out = "TOTAL 24\n16\n14\n17\n15\n"
    with patch("scripts.wikimedia_upload_status.ssm_run", return_value=out):
        line = _format_slots_line(object())
    assert line == "SDC slots: ~8 free of 24 (16 held)"


def test_format_slots_line_none_when_no_slot_dir():
    """No slot dir (no budget-enabled session has run) → no line."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import _format_slots_line

    with patch("scripts.wikimedia_upload_status.ssm_run", return_value="NODIR\n"):
        assert _format_slots_line(object()) is None


def test_format_slots_line_none_when_lslocks_absent():
    """lslocks missing → NODATA → no line (rather than a misleading "all
    free" from grep -c on empty stdin)."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import _format_slots_line

    with patch("scripts.wikimedia_upload_status.ssm_run", return_value="NODATA\n"):
        assert _format_slots_line(object()) is None
