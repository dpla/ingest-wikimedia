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
    total_ordinals: int = 0,
):
    """Build a fake `ssm_run` that returns the precheck + counts payload
    `get_phase_and_progress` expects, with the named log file and counts.

    Sequence (matches the real two-call flow):
      1. precheck — `session_created\nlog_filename`
      2. main — `now\nmtime\nSEP\ntail\nSEP\n<6 lines of awk + wc>\nSEP\n<total_ordinals>`

    ``awk_counts`` is the five-element awk-emitted prefix in order:
    ``[dpla_id_count, uploaded_count, skipped_count, counts_marker,
    ordinal_count]``. The trailing ``csv_total`` from ``wc -l`` is
    appended automatically.

    ``mtime`` is parameterised so tests can verify the mtime tiebreak in
    ``main`` — an "aborted phase" fake with an earlier mtime must be
    distinguishable from an "active phase" fake with a later mtime, even
    though both produce a non-COUNTS-marker phase string.  ``now`` is
    kept at the same value so the staleness suffix doesn't fire (it
    would change the phase-string assertions in unrelated callers).

    ``total_ordinals`` is the file-level denominator the helper now
    derives from the corresponding download log; default 0 mirrors the
    "no download log found" case (legacy sessions or pre-PR-272 logs),
    where the Upload and SDC branches fall back to item-count.
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
        body += f"{sep}\n{total_ordinals}\n"
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
            "__WM_SEP__\n2\n0\n0\n0\n0\n5\n"
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
        return "1700000000\n1700000000\n__WM_SEP__\n.\n__WM_SEP__\n1\n0\n0\n0\n0\n3\n"

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
        awk_counts=[3, 0, 0, 0, 0],
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
        awk_counts=[100, 0, 0, 0, 0],
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
        awk_counts=[10, 0, 0, 1, 0],  # 10 items, COUNTS marker present
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
        awk_counts=[0, 0, 0, 0, 0],
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


def test_get_phase_and_progress_reports_sdc_queued_when_waiting():
    """An -sdc.log with no items logged yet whose tail is the slot-budget
    wait message is "queued" (parked behind the cap), not "starting..."."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    fake = _fake_ssm_for_phase(
        log_filename="20260525-200000-minnesota-sdc.log",
        awk_counts=[0, 0, 0, 0, 0],
        csv_total=10,
        tail=" -- All 16 worker slots busy; waiting for capacity.",
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        phase, _ = get_phase_and_progress(
            client=None,
            session="wikimedia-minnesota",
            hub="minnesota",
            label="minnesota",
        )
    assert phase == "SDC syncing (queued) ⏸ waiting on slots"


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
        awk_counts=[250, 0, 0, 0, 0],  # no COUNTS marker → not complete
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
        awk_counts=[3101, 0, 0, 0, 0],
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
    assert line == "Worker slots: ~8 free of 24 (16 held)"


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


# ---------------------------------------------------------------------------
# find_active_label — single-SSM-call refactor of the per-label walk


def test_find_active_label_picks_freshest_log_across_labels():
    """Returns the label whose log was most recently modified. The single
    SSM call returns ``<mtime> <filename>`` for the freshest match; the
    helper parses out the label."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import find_active_label

    # Latest-mtime log file in the partner dir is the upload log for
    # bpl+phillips-academy. The helper should pick that label.
    with patch(
        "scripts.wikimedia_upload_status.ssm_run",
        return_value="1700005000.000000000 20260528-091047-bpl+phillips-academy-upload.log",
    ):
        result = find_active_label(
            client=None,
            labels=["bpl+phillips-academy", "bpl+boston-city-archives"],
        )
    assert result is not None
    label, mtime = result
    assert label == "bpl+phillips-academy"
    assert mtime == 1700005000


def test_find_active_label_returns_none_when_no_logs_match():
    """An empty ``find`` output (no log files for any label yet) returns
    ``None`` so the caller can report ``Generating IDs`` for a brand-new
    session that hasn't yet written any downstream phase log."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import find_active_label

    with patch("scripts.wikimedia_upload_status.ssm_run", return_value=""):
        result = find_active_label(client=None, labels=["bpl+phillips-academy"])
    assert result is None


def test_find_active_label_returns_none_for_empty_label_list():
    """Defensive: empty label list short-circuits without an SSM round
    trip. The helper is reached only via `parse_session_labels`, which
    can return an empty list on a malformed session name."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import find_active_label

    with patch("scripts.wikimedia_upload_status.ssm_run") as ssm:
        result = find_active_label(client=None, labels=[])
    assert result is None
    assert not ssm.called, "Empty label list must skip the SSM round trip"


def test_find_active_label_groups_multi_hub_labels_into_single_find():
    """Regression: when labels span multiple hubs (the multi-institution
    batch case that motivated this refactor), the helper issues exactly
    one SSM round trip whose ``find`` invocation lists every relevant
    partner log directory. The previous design called
    ``get_phase_and_progress`` per label — O(labels) round trips."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import find_active_label

    captured: list[str] = []

    def fake_ssm_run(_client, command, **_kwargs):
        captured.append(command)
        return "1700000000.0 20260528-091047-bpl+phillips-academy-upload.log"

    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake_ssm_run):
        find_active_label(
            client=None,
            labels=[
                "bpl+phillips-academy",
                "nara+ronald-reagan-library",
                "texas+harrison-county-historical-museum",
            ],
        )
    assert len(captured) == 1, "Multi-hub labels must use one SSM round trip"
    cmd = captured[0]
    # All three hubs' log directories should appear in the single find expression.
    assert "bpl/logs" in cmd
    assert "nara/logs" in cmd
    assert "texas/logs" in cmd


def test_find_active_label_picks_active_when_earlier_label_aborted():
    """Regression analog of the pre-refactor ``main_picks_latest_active_label``
    test: an earlier label whose phase aborted without a COUNTS marker
    must NOT eclipse a later label whose phase is currently progressing.

    Under the new design, ``find_active_label`` resolves this purely by
    log-mtime: the aborted phase's last log write is hours stale, the
    active phase's was seconds ago. The freshest log file wins, and
    that's the active label."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import find_active_label

    # ``find ... | sort -rn | head -1`` returns the highest-mtime line;
    # we just have to feed it that one line. The active CFLA SDC log
    # would be ranked above the aborted Clinton SDC log because its
    # mtime is later.
    with patch(
        "scripts.wikimedia_upload_status.ssm_run",
        return_value="1700018000.0 20260528-140954-nara+center-for-legislative-archives-sdc.log",
    ):
        result = find_active_label(
            client=None,
            labels=[
                "nara+william-j-clinton-library",
                "nara+dwight-d-eisenhower-library",
                "nara+center-for-legislative-archives",
            ],
        )
    assert result is not None
    label, _ = result
    assert label == "nara+center-for-legislative-archives", (
        "Active later label must win the freshest-log selection"
    )


def test_find_active_label_rejects_filename_not_in_label_list():
    """Defensive: if the ``find`` regex's alternation somehow returns a
    label that isn't in ``labels`` (regex prefix collision in a edge
    case), the helper returns ``None`` rather than reporting a stray
    label."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import find_active_label

    with patch(
        "scripts.wikimedia_upload_status.ssm_run",
        return_value="1700000000.0 20260528-091047-some+other-hub-upload.log",
    ):
        result = find_active_label(client=None, labels=["bpl+phillips-academy"])
    assert result is None


# ---------------------------------------------------------------------------
# Slack output safety — multi-block splitting


def _capture_slack_post():
    """Build a ``(captured, fake_post)`` pair for the ``post_to_slack``
    integration tests. Each test only needs to read one or two fields
    off the captured payload, so a tiny mock that stashes the body is
    sufficient — no full ``requests.Response`` surface needed."""

    class _R:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"ok": True}

    captured: dict[str, object] = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["payload"] = json
        return _R()

    return captured, fake_post


def test_format_rows_into_blocks_single_block_when_under_limit():
    from scripts.wikimedia_upload_status import _format_rows_into_blocks

    rows = [
        ("bpl+phillips-academy", "Uploading (10 / 100 items, ~10.0%)"),
        ("nara+ronald-reagan-library", "SDC syncing (5 / 50, ~10.0%)"),
    ]
    blocks = _format_rows_into_blocks(rows)
    assert len(blocks) == 1
    assert blocks[0]["type"] == "section"
    assert "bpl+phillips-academy" in blocks[0]["text"]["text"]
    assert "nara+ronald-reagan-library" in blocks[0]["text"]["text"]


def test_format_rows_into_blocks_splits_when_over_block_limit():
    """If the total text would exceed a single block's char budget,
    rows are spread across multiple section blocks rather than
    triggering Slack's ``invalid_blocks`` rejection."""
    from scripts.wikimedia_upload_status import (
        _SLACK_BLOCK_SOFT_LIMIT,
        _format_rows_into_blocks,
    )

    # Build enough rows that the cumulative text definitely exceeds one
    # block's budget. Each row formats to ~100 chars; 50 rows clears
    # the soft limit even with the active-label display ids.
    rows = [(f"session-{i:03d}", "Uploading (1 / 1)" * 5) for i in range(50)]
    blocks = _format_rows_into_blocks(rows)
    assert len(blocks) >= 2, "Many rows must split into multiple blocks"
    for block in blocks:
        assert block["type"] == "section"
        assert len(block["text"]["text"]) <= _SLACK_BLOCK_SOFT_LIMIT, (
            "Each block's text must stay under the per-block limit"
        )


def test_post_to_slack_payload_contains_multiple_section_blocks_for_busy_day():
    """End-to-end: many rows produce a valid payload with multiple
    section blocks (header + N sections + optional context), each
    section well under Slack's per-block char limit."""
    import json
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import (
        _SLACK_BLOCK_SOFT_LIMIT,
        post_to_slack,
    )

    rows = [(f"session-{i:03d}", "Uploading (1 / 1)" * 5) for i in range(60)]
    captured, fake_post = _capture_slack_post()
    with patch("scripts.wikimedia_upload_status.requests.post", side_effect=fake_post):
        post_to_slack("tok-xxx", rows)

    payload = captured["payload"]
    assert payload["channel"] == "C02HEU2L3"
    assert payload["text"] == "Wikimedia Upload Status"
    section_blocks = [b for b in payload["blocks"] if b.get("type") == "section"]
    assert len(section_blocks) >= 2, "Busy day should produce multiple section blocks"
    for block in section_blocks:
        assert len(block["text"]["text"]) <= _SLACK_BLOCK_SOFT_LIMIT
    for block in section_blocks:
        assert block["text"]["text"].strip(), "Empty section block emitted"
    assert json.dumps(payload)


def test_main_rows_use_display_id_from_fetch_not_session_name():
    """Regression: ``fetch()`` returns ``(display_id, phase)`` where
    ``display_id`` is the active label, NOT the tmux session name. The
    rows-construction in ``main()`` must therefore key the
    intermediate results dict by session name (so ``sessions``-ordered
    lookup works), not by the display id returned from ``fetch``.

    Without this mapping the payload's rows are empty — caught by
    CodeRabbit on PR #328 after the first push of the refactor."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import main

    sessions_out = "wikimedia-bpl+phillips-academy: 1 windows\n"
    captured, fake_post = _capture_slack_post()

    # Patch the helpers ``fetch`` calls so the real ``fetch`` returns
    # a deterministic ``(label, phase)`` pair where label differs from
    # the session name. The real ThreadPoolExecutor runs, so we
    # exercise the actual row-construction path.
    with (
        patch.dict(
            "os.environ",
            {"DPLA_SLACK_BOT_TOKEN": "tok-xxx", "NOTIFY_IF_IDLE": "false"},
        ),
        patch("scripts.wikimedia_upload_status.boto3.client", return_value=object()),
        patch(
            "scripts.wikimedia_upload_status.ssm_run",
            return_value=sessions_out,
        ),
        patch(
            "scripts.wikimedia_upload_status.fetch_memory_snapshot",
            return_value=None,
        ),
        patch(
            "scripts.wikimedia_upload_status._format_slots_line",
            return_value=None,
        ),
        patch(
            "scripts.wikimedia_upload_status.find_active_label",
            return_value=("bpl+phillips-academy", 1700000000),
        ),
        patch(
            "scripts.wikimedia_upload_status.get_phase_and_progress",
            return_value=("Uploading (1 / 1, ~100.0%)", 1700000000),
        ),
        patch(
            "scripts.wikimedia_upload_status.requests.post",
            side_effect=fake_post,
        ),
    ):
        main()

    payload = captured["payload"]
    section_blocks = [b for b in payload["blocks"] if b.get("type") == "section"]
    assert section_blocks, (
        "Slack post must include at least one section block — empty "
        "rows would indicate the session-name → display-id mapping is "
        "broken (the bug CodeRabbit caught on PR #328)."
    )
    text = section_blocks[0]["text"]["text"]
    assert "bpl+phillips-academy" in text
    assert "Uploading" in text


# ---------------------------------------------------------------------------
# Upload-phase file-level progress (PR: file progress + position-in-chain)


def test_upload_progress_reports_file_level_when_download_log_available():
    """When the downloader has finished and the per-item `Item <id>: N
    ordinals` summary lines are present in the download log, the Upload
    phase status reports file-level progress (uploaded + skipped vs.
    total ordinals) rather than item-level. A 100-page newspaper item
    is no longer indistinguishable from a 1-image photo in the readout.
    """
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    # 1500 of 6000 ordinals processed (1200 uploaded + 300 skipped),
    # ~25%. Item count is irrelevant when total_ordinals > 0.
    fake = _fake_ssm_for_phase(
        log_filename="20260528-091047-bpl+phillips-academy-upload.log",
        # [dpla_id_count, uploaded, skipping, counts]
        awk_counts=[200, 1200, 300, 0, 0],
        csv_total=400,  # items — used as fallback only
        total_ordinals=6000,
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        phase, _ = get_phase_and_progress(
            client=None,
            session="wikimedia-bpl+phillips-academy",
            hub="bpl",
            label="bpl+phillips-academy",
        )
    assert phase is not None
    # File-level: (1200 + 300) / 6000 = 25.0%
    assert "1,500 / 6,000 files" in phase, phase
    assert "~25.0%" in phase, phase
    # Item-level fallback string must NOT appear when file-level is in scope.
    assert "items" not in phase, (
        f"file-level reporting must replace item-level: got {phase!r}"
    )


def test_upload_progress_falls_back_to_item_level_when_no_download_log():
    """Legacy sessions (pre-PR-272 logs) and sessions whose download
    phase ran under a separate label have ``total_ordinals == 0``.
    The Upload phase falls back to the item-level denominator so the
    readout doesn't degrade to ``0 / 0 ?%``."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    fake = _fake_ssm_for_phase(
        log_filename="20260528-091047-bpl+phillips-academy-upload.log",
        awk_counts=[50, 30, 5, 0, 0],
        csv_total=200,
        total_ordinals=0,  # no download log found
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        phase, _ = get_phase_and_progress(
            client=None,
            session="wikimedia-bpl+phillips-academy",
            hub="bpl",
            label="bpl+phillips-academy",
        )
    assert phase is not None
    # Item-level: 50 / 200 = 25.0%
    assert "50 / 200 items" in phase, phase
    assert "~25.0%" in phase, phase


def test_sdc_progress_reports_file_level_when_download_log_available():
    """Mirror of the Upload-phase file-level test for SDC. When the
    SDC log carries per-ordinal ``-- Ordinal N:`` markers and the
    download log gives a total file count, SDC progress is file-level.
    Multi-page newspaper items would otherwise hide the actual SDC
    work — the bot writes structured data on every ordinal, not just
    once per item."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    # 400 items processed, 1,800 ordinals touched, 6,000 total = 30.0%
    fake = _fake_ssm_for_phase(
        log_filename="20260622-161651-texas+stephen-f-austin-sdc.log",
        awk_counts=[400, 0, 0, 0, 1800],
        csv_total=939,
        total_ordinals=6000,
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        phase, _ = get_phase_and_progress(
            client=None,
            session="wikimedia-texas+stephen-f-austin",
            hub="texas",
            label="texas+stephen-f-austin",
        )
    assert phase is not None
    assert "1,800 / 6,000 files" in phase, phase
    assert "~30.0%" in phase, phase
    assert "items" not in phase, (
        f"file-level reporting must replace item-level in SDC: got {phase!r}"
    )


def test_sdc_progress_falls_back_to_item_level_when_no_download_log():
    """SDC's item-level fallback fires when no download log was found
    (legacy sessions, or download phase lives elsewhere)."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    fake = _fake_ssm_for_phase(
        log_filename="20260622-161651-texas+stephen-f-austin-sdc.log",
        awk_counts=[400, 0, 0, 0, 1800],
        csv_total=939,
        total_ordinals=0,
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        phase, _ = get_phase_and_progress(
            client=None,
            session="wikimedia-texas+stephen-f-austin",
            hub="texas",
            label="texas+stephen-f-austin",
        )
    assert phase is not None
    assert "400 / 939 items" in phase, phase


def test_sdc_progress_falls_back_to_item_level_when_no_ordinal_markers_yet():
    """Defensive: a download log can be present (total_ordinals > 0)
    while the SDC log is too new to have emitted any
    ``-- Ordinal N:`` markers yet (just-started session — items are
    starting but their first ordinals haven't been touched). Use the
    item-level form rather than reporting 0.0%, which would look
    visually identical to "not started" while the session is in fact
    running its first item."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    fake = _fake_ssm_for_phase(
        log_filename="20260622-161651-texas+stephen-f-austin-sdc.log",
        awk_counts=[5, 0, 0, 0, 0],
        csv_total=939,
        total_ordinals=6000,
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        phase, _ = get_phase_and_progress(
            client=None,
            session="wikimedia-texas+stephen-f-austin",
            hub="texas",
            label="texas+stephen-f-austin",
        )
    assert phase is not None
    assert "5 / 939 items" in phase, phase


def test_upload_progress_passes_label_glob_into_download_log_lookup():
    """The download-log lookup must scope to THIS label, not the whole
    log directory — otherwise multi-institution batches would sum
    ordinal counts from sibling labels' download logs and over-count
    the denominator."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    captured: list[str] = []

    def fake_ssm_run(_client, command, **_kwargs):
        captured.append(command)
        if len(captured) == 1:
            return "1700000000\n20260528-091047-bpl+phillips-academy-upload.log\n"
        return (
            "1700000000\n1700000000\n__WM_SEP__\n.\n__WM_SEP__\n"
            "10\n5\n2\n0\n0\n50\n__WM_SEP__\n200\n"
        )

    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake_ssm_run):
        get_phase_and_progress(
            client=None,
            session="wikimedia-bpl+phillips-academy",
            hub="bpl",
            label="bpl+phillips-academy",
        )
    main_cmd = captured[1]
    # The download-log glob must include the specific label, not just
    # `*-download.log`.
    assert "bpl+phillips-academy-download.log" in main_cmd, main_cmd


def test_get_phase_and_progress_rejects_non_slug_label():
    """Defense-in-depth: the label is interpolated unquoted into the
    download-log glob, so the function MUST refuse non-slug-shaped
    labels rather than relying on caller discipline. A maliciously
    crafted label like ``"bpl;rm -rf /tmp"`` would otherwise produce
    a shell-command injection at the SSM round-trip.

    The slug shape is ``[a-z0-9+\\-]+`` — what ``parse_session_labels``
    and ``PARTNER_HUBS`` produce. Anything else raises ``ValueError``
    at the function boundary."""
    import pytest

    from scripts.wikimedia_upload_status import get_phase_and_progress

    for bad in (
        "bpl; echo pwned",  # command separator
        "bpl|echo pwned",  # pipe
        "bpl$(id)",  # command substitution
        "bpl`id`",  # backtick command substitution
        "bpl 'a",  # quote + space
        "bpl/../etc/passwd",  # path traversal
        "bpl*",  # raw glob metachar
        "BPL",  # uppercase (slug is lowercase only)
        "",  # empty
    ):
        with pytest.raises(ValueError, match="slug-shaped"):
            get_phase_and_progress(
                client=None, session="wikimedia-x", hub="bpl", label=bad
            )


def test_download_log_glob_is_not_single_quoted():
    """Regression: the original implementation shlex.quote'd the glob
    pattern, which wraps it in single quotes and disables shell glob
    expansion. ``ls -t '*-foo-download.log'`` looks for a literal file
    named ``*-foo-download.log`` rather than expanding the ``*``. The
    bug silently fell back to item-level reporting in production
    because total_ordinals was always 0.

    Pin the contract: the glob's ``*`` must be unquoted in the rendered
    SSM command so the shell expands it. Labels are pure slug
    characters with no shell metacharacters, so direct interpolation
    is safe."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    captured: list[str] = []

    def fake_ssm_run(_client, command, **_kwargs):
        captured.append(command)
        if len(captured) == 1:
            return "1700000000\n20260528-091047-bpl+phillips-academy-upload.log\n"
        return (
            "1700000000\n1700000000\n__WM_SEP__\n.\n__WM_SEP__\n"
            "10\n5\n2\n0\n0\n50\n__WM_SEP__\n200\n"
        )

    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake_ssm_run):
        get_phase_and_progress(
            client=None,
            session="wikimedia-bpl+phillips-academy",
            hub="bpl",
            label="bpl+phillips-academy",
        )
    main_cmd = captured[1]
    # The bug shape was `ls -t <log_dir>/'*-bpl+phillips-academy-download.log'`
    # — i.e. the literal `'*-` pattern. The fix renders it as
    # `ls -t <log_dir>/*-bpl+phillips-academy-download.log` (no quotes
    # around the asterisk), so shell glob expansion fires.
    assert "/'*-" not in main_cmd, (
        "download-log glob must not be wrapped in single quotes — "
        f"shell glob expansion would be disabled. Rendered: {main_cmd!r}"
    )
    # Positive check: the unquoted glob is present.
    assert "/*-bpl+phillips-academy-download.log" in main_cmd, main_cmd


def test_total_ordinals_awk_counts_universal_downloading_marker():
    """The total-ordinals denominator must come from a marker that's
    present in EVERY version of the downloader's log, not just
    post-PR-272 logs. NARA-style large hubs run their download phase
    once and iterate upload + SDC many times — the download log stays
    in its original form forever, so the status script must work
    against logs from before PR #272 added the per-item ``Item <id>:
    N ordinals`` summary line.

    The ``Downloading <partner> <id> <ordinal> from <url>`` line at
    ``downloader.py:532`` has been emitted unconditionally since the
    downloader was first written, so it's the right universal source.
    Pin the contract: the rendered SSM command must use that pattern,
    not the post-PR-272-only Item-summary one."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    captured: list[str] = []

    def fake_ssm_run(_client, command, **_kwargs):
        captured.append(command)
        if len(captured) == 1:
            return "1700000000\n20260528-091047-bpl+phillips-academy-upload.log\n"
        return (
            "1700000000\n1700000000\n__WM_SEP__\n.\n__WM_SEP__\n"
            "10\n5\n2\n0\n0\n50\n__WM_SEP__\n200\n"
        )

    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake_ssm_run):
        get_phase_and_progress(
            client=None,
            session="wikimedia-bpl+phillips-academy",
            hub="bpl",
            label="bpl+phillips-academy",
        )
    main_cmd = captured[1]
    # The new pattern that works on legacy AND current downloader logs.
    assert "Downloading [a-z0-9-]+ [a-f0-9]+ [0-9]+ from" in main_cmd, main_cmd
    # The old post-PR-272-only pattern must NOT be present — it would
    # silently return 0 against any download log that predates PR #272.
    assert "Item [a-f0-9]+: [0-9]+ ordinals" not in main_cmd, main_cmd


def test_download_phase_classifies_no_media_skip_marker_as_active():
    """All-no-media partners — e.g. a maintain-mode pass over a hub whose
    items have no eligible media — emit the per-item ``No media;
    skipping.`` marker added at ``tools/downloader.py:484``. The status
    script must treat that marker as evidence the downloader is
    actively iterating, not classify the run as ``Stalled``. Without
    this branch in the tail-check, the prior tests asserting ``Stalled``
    only on truly missing markers would mask the calhoun-style
    misclassification."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    fake = _fake_ssm_for_phase(
        log_filename="20260623-200436-georgia+calhoun-gordon-county-library-download.log",
        awk_counts=[42311, 0, 0, 0, 0],  # 42,311 items iterated, no COUNTS yet
        csv_total=50120,
        tail="[INFO] 16:50:00: No media; skipping.",
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        phase, _ = get_phase_and_progress(
            client=None,
            session="wikimedia-georgia+calhoun-gordon-county-library",
            hub="georgia",
            label="georgia+calhoun-gordon-county-library",
        )
    assert phase is not None
    assert phase.startswith("Downloading"), phase
    assert "Stalled" not in phase, phase
    assert "42,311 / 50,120 items" in phase, phase
    assert "~84.4%" in phase, phase


def test_fetch_position_annotation_for_multi_label_batch():
    """Multi-label batch sessions get a `[<pos>/<total>]` suffix on the
    active label, replacing the prior `(+N more)` form. Lets the
    reader see at a glance how far along the chain a session is."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import main

    sessions_out = "wikimedia-texas+a+texas+b+texas+c+texas+d: 1 windows\n"
    captured, fake_post = _capture_slack_post()
    with (
        patch.dict(
            "os.environ",
            {"DPLA_SLACK_BOT_TOKEN": "tok", "NOTIFY_IF_IDLE": "false"},
        ),
        patch("scripts.wikimedia_upload_status.boto3.client", return_value=object()),
        patch("scripts.wikimedia_upload_status.ssm_run", return_value=sessions_out),
        patch(
            "scripts.wikimedia_upload_status.fetch_memory_snapshot", return_value=None
        ),
        patch("scripts.wikimedia_upload_status._format_slots_line", return_value=None),
        # Active label is the 3rd of 4 (texas+c).
        patch(
            "scripts.wikimedia_upload_status.find_active_label",
            return_value=("texas+c", 1700000000),
        ),
        patch(
            "scripts.wikimedia_upload_status.get_phase_and_progress",
            return_value=("Uploading (10 / 100 files, ~10.0%)", 1700000000),
        ),
        patch("scripts.wikimedia_upload_status.requests.post", side_effect=fake_post),
    ):
        main()

    section_text = next(
        b["text"]["text"]
        for b in captured["payload"]["blocks"]
        if b.get("type") == "section"
    )
    # The position annotation must appear and be position-3-of-4, not "(+3 more)".
    assert "texas+c [3/4]" in section_text, section_text
    assert "(+" not in section_text, (
        f"old ambiguous suffix must be gone: {section_text!r}"
    )


def test_fetch_no_position_annotation_for_single_label_session():
    """Single-label sessions don't need the position annotation —
    `[1/1]` would just be noise. Confirm it stays bare."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import main

    sessions_out = "wikimedia-bpl+phillips-academy: 1 windows\n"
    captured, fake_post = _capture_slack_post()
    with (
        patch.dict(
            "os.environ",
            {"DPLA_SLACK_BOT_TOKEN": "tok", "NOTIFY_IF_IDLE": "false"},
        ),
        patch("scripts.wikimedia_upload_status.boto3.client", return_value=object()),
        patch("scripts.wikimedia_upload_status.ssm_run", return_value=sessions_out),
        patch(
            "scripts.wikimedia_upload_status.fetch_memory_snapshot", return_value=None
        ),
        patch("scripts.wikimedia_upload_status._format_slots_line", return_value=None),
        patch(
            "scripts.wikimedia_upload_status.find_active_label",
            return_value=("bpl+phillips-academy", 1700000000),
        ),
        patch(
            "scripts.wikimedia_upload_status.get_phase_and_progress",
            return_value=("Uploading (10 / 100 files, ~10.0%)", 1700000000),
        ),
        patch("scripts.wikimedia_upload_status.requests.post", side_effect=fake_post),
    ):
        main()

    section_text = next(
        b["text"]["text"]
        for b in captured["payload"]["blocks"]
        if b.get("type") == "section"
    )
    assert "bpl+phillips-academy" in section_text
    # No position bracket on a single-label session.
    assert "[1/1]" not in section_text
    assert "[/" not in section_text
