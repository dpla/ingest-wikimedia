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
    """The five recognised phase suffixes: download, upload, sdc,
    drain-deferred, drain-deferred-opportunistic. Anything else (e.g.
    legacy retirer logs) must not match."""
    from scripts.wikimedia_upload_status import log_filename_pattern_for_label

    pattern = re.compile(log_filename_pattern_for_label("nara"))
    assert pattern.search("20260522-100000-nara-download.log")
    assert pattern.search("20260522-100000-nara-upload.log")
    assert pattern.search("20260522-100000-nara-sdc.log")
    assert pattern.search("20260522-100000-nara-drain-deferred.log")
    assert pattern.search("20260522-100000-nara-drain-deferred-opportunistic.log")
    # Other phases (e.g. legacy retirer logs) must NOT match
    assert not pattern.search("20251220-012010-nara-retirer.log")
    assert not pattern.search("20260522-100000-nara-fix.log")
    # Guard against a partial-match footgun: the pattern must be anchored
    # so that "nara-drain-deferred-oops.log" is NOT interpreted as a
    # valid drain-deferred variant.
    assert not pattern.search("20260522-100000-nara-drain-deferred-oops.log")


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


def _fake_ssm_for_drain(
    *,
    log_filename: str,
    tail: str,
    queued: int,
    lock_acquired_count: int | None = None,
    mtime: int = 1700000000,
    session_created: int = 0,
):
    """Two-call SSM fake for the drain-deferred phase code path.

    Call 1 (precheck): ``session_created\\nlog_filename\\n``.
    Call 2 (drain state): four SEP-delimited sections —
    ``{mtime}<SEP>{tail}<SEP>{queued}<SEP>{lock_acquired_count}`` where
    SEP is ``__WM_DRAIN_SEP__``. The 4th section is the whole-file
    ``grep -c`` of the lock-acquired marker (decoupled from ``tail`` so
    the reporter can't be fooled by tail-window eviction on long
    drains).

    ``lock_acquired_count`` defaults to being derived from ``tail`` —
    mirroring the common case where the marker is still in-window — but
    can be set explicitly to exercise the tail-eviction regression
    (marker present in the full file, absent from the tail).
    """
    call = [0]
    sep = "__WM_DRAIN_SEP__"
    if lock_acquired_count is None:
        lock_acquired_count = 1 if "Drain-phase host lock acquired." in tail else 0

    def fake(_client, _cmd, **_kw):
        call[0] += 1
        if call[0] == 1:
            return f"{session_created}\n{log_filename}\n"
        return (
            f"{mtime}\n{sep}\n{tail}\n{sep}\n{queued}\n{sep}\n{lock_acquired_count}\n"
        )

    return fake


def test_drain_deferred_reports_waiting_for_lock():
    """Session queued behind another partner's drain: no
    ``Drain-phase host lock acquired.`` line yet — must be flagged as
    lock-waiting rather than as SDC-complete-and-idle."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    fake = _fake_ssm_for_drain(
        log_filename="20260705-120631-northwest-heritage+oregon-state-archives-drain-deferred.log",
        tail=(
            "[INFO] 12:06:31: Drain-deferred: sidecar for partner "
            "northwest-heritage has 8 item(s) queued; acquiring host lock "
            "(mode: patient).\n"
            "[INFO] 12:06:31: Acquiring drain-phase host lock at "
            "/home/ec2-user/ingest-wikimedia/.drain-lock (blocking until "
            "available)…"
        ),
        queued=8,
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        phase, _mtime = get_phase_and_progress(
            client=None,
            session="wikimedia-northwest-heritage+oregon-state-archives",
            hub="northwest-heritage",
            label="northwest-heritage+oregon-state-archives",
        )
    assert "waiting for host lock" in phase
    assert "8 queued" in phase


def test_drain_deferred_reports_throttle_state_when_holding_lock():
    """Session holding the lock and polling ``Category:Duplicate``:
    surface the live throttle numbers so operators can gauge how
    close the resume gate is to opening."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    fake = _fake_ssm_for_drain(
        log_filename="20260705-092303-drain-ohio-drain-deferred.log",
        tail=(
            "[INFO] 14:58:16: Drain-phase host lock acquired.\n"
            "[INFO] 14:58:16: Category:Duplicate at 979 (>= resume "
            "threshold 900); waiting 300s before retrying deferred "
            "duplicate-tags.\n"
            "[INFO] 15:03:17: Category:Duplicate at 954 (>= resume "
            "threshold 900); waiting 300s before retrying deferred "
            "duplicate-tags."
        ),
        queued=42,
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        phase, _mtime = get_phase_and_progress(
            client=None,
            session="wikimedia-ohio",
            hub="ohio",
            label="ohio",
        )
    assert "42 queued" in phase
    # Uses the MOST RECENT throttle poll (954), not the earliest (979).
    assert "954" in phase
    assert "900" in phase


def test_drain_deferred_reports_opportunistic_capacity_skip():
    """Opportunistic drain that hit ``at capacity`` and exited fast is
    terminal — surface how many items remain deferred to the
    batch-terminal patient drain, not a still-working state."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    fake = _fake_ssm_for_drain(
        log_filename="20260705-092302-bpl+phillips-academy-drain-deferred-opportunistic.log",
        tail=(
            "[INFO] 09:23:02: Drain-phase host lock acquired.\n"
            "[INFO] 09:23:03: Drain-deferred (opportunistic): "
            "Category:Duplicate at capacity; 7 item(s) remain in sidecar "
            "for the batch's terminal drain."
        ),
        queued=7,
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        phase, _mtime = get_phase_and_progress(
            client=None,
            session="wikimedia-bpl+phillips-academy",
            hub="bpl",
            label="bpl+phillips-academy",
        )
    assert "opportunistic" in phase
    assert "7" in phase


def test_drain_deferred_reports_complete_when_sidecar_drained():
    """Sidecar drained to empty + ``Drain-deferred: complete.`` marker
    → the phase is terminal and reports completion so the walker can
    move on."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    fake = _fake_ssm_for_drain(
        log_filename="20260705-092303-drain-ohio-drain-deferred.log",
        tail=(
            "[INFO] 15:00:00: Drain-deferred: complete. Emitted "
            "42 item(s) over 3600 seconds."
        ),
        queued=0,
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        phase, _mtime = get_phase_and_progress(
            client=None,
            session="wikimedia-ohio",
            hub="ohio",
            label="ohio",
        )
    assert phase.startswith("Drain complete")


def test_drain_deferred_lock_marker_read_from_full_log_not_tail():
    """Regression (CR #368): a patient drain that has held the lock for
    hours has scrolled the one-shot ``Drain-phase host lock acquired.``
    line out of the tail window — the tail is now all throttle-poll
    lines. The lock state must come from the FULL-log grep count (4th
    SSM section), so this session still reports its throttle state, NOT
    a false 'waiting for host lock'."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    fake = _fake_ssm_for_drain(
        log_filename="20260705-092303-drain-ohio-drain-deferred.log",
        # Tail is ONLY throttle polls — lock-acquired line evicted.
        tail=(
            "[INFO] 14:53:17: Category:Duplicate at 958 (>= resume "
            "threshold 900); waiting 300s before retrying deferred "
            "duplicate-tags.\n"
            "[INFO] 14:58:17: Category:Duplicate at 954 (>= resume "
            "threshold 900); waiting 300s before retrying deferred "
            "duplicate-tags."
        ),
        queued=42,
        # But the full-file grep still finds the marker.
        lock_acquired_count=1,
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        phase, _mtime = get_phase_and_progress(
            client=None,
            session="wikimedia-ohio",
            hub="ohio",
            label="ohio",
        )
    assert "waiting for host lock" not in phase, (
        "lock marker evicted from tail must not be misread as lock-waiting"
    )
    assert "954" in phase and "900" in phase


def test_drain_deferred_returns_none_for_stale_log_predating_session():
    """Regression (CR #368): when the drain log predates the current
    session (log_mtime < session_created), the helper must return the
    ``None`` sentinel — matching every other branch of
    get_phase_and_progress — so the caller's fallback fires instead of
    rendering a blank phase string."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    fake = _fake_ssm_for_drain(
        log_filename="20260705-092303-drain-ohio-drain-deferred.log",
        tail="[INFO] 09:23:02: Drain-phase host lock acquired.",
        queued=5,
        mtime=1700000000,
        session_created=1700000000 + 3600,  # session started AFTER this log
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake):
        result = get_phase_and_progress(
            client=None,
            session="wikimedia-ohio",
            hub="ohio",
            label="ohio",
        )
    assert result == (None, 0)


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


def test_fetch_slot_snapshot_reports_free_headroom_and_holds_by_label():
    """The snapshot returns median-smoothed headroom PLUS a per-session
    map of holds derived from the final ``HOLDER <label>`` block. Both
    pieces travel in a single SSM roundtrip because the saturated
    per-session view is only ever useful with the aggregate — sending
    them separately would double-charge Slack's 3-second ack budget."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import SlotSnapshot, _fetch_slot_snapshot

    # TOTAL 24; three count-only samples [22, 24, 23]; final structured
    # pass reports COUNT 24 + six HOLDER labels (one uploader + a
    # 6-worker sdc-sync would look like this in the wild).
    out = (
        "TOTAL 24\n"
        "22\n"
        "24\n"
        "23\n"
        "COUNT 24\n"
        "HOLDER nara+franklin-d-roosevelt-library\n"
        "HOLDER nara+jimmy-carter-library\n"
        "HOLDER nara+jimmy-carter-library\n"
        "HOLDER nara+jimmy-carter-library\n"
        "HOLDER nara+jimmy-carter-library\n"
        "HOLDER nara+jimmy-carter-library\n"
        "HOLDER nara+jimmy-carter-library\n"
        "HOLDER ohio+state-library-of-ohio\n"
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", return_value=out):
        snap = _fetch_slot_snapshot(object())
    assert isinstance(snap, SlotSnapshot)
    # Median of [22, 24, 23, 24] = 23.5 → 24 held → 0 free.
    assert snap.line == "Worker slots: ~0 free of 24 (24 held)"
    assert snap.free == 0
    assert snap.holds_by_label == {
        "nara+franklin-d-roosevelt-library": 1,
        "nara+jimmy-carter-library": 6,
        "ohio+state-library-of-ohio": 1,
    }


def test_fetch_slot_snapshot_none_when_no_slot_dir():
    """No slot dir (no budget-enabled session has run) → no snapshot."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import _fetch_slot_snapshot

    with patch("scripts.wikimedia_upload_status.ssm_run", return_value="NODIR\n"):
        assert _fetch_slot_snapshot(object()) is None


def test_fetch_slot_snapshot_none_when_lslocks_absent():
    """lslocks missing → NODATA → no snapshot (rather than a misleading
    "all free" from grep -c on empty stdin)."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import _fetch_slot_snapshot

    with patch("scripts.wikimedia_upload_status.ssm_run", return_value="NODATA\n"):
        assert _fetch_slot_snapshot(object()) is None


def test_fetch_slot_snapshot_aggregate_scopes_to_shared_pool_only():
    """Regression (CR flagged on PR #366): ``held`` / ``free`` / ``line``
    must reflect ONLY the shared 24-slot pool — the ``TOTAL`` line is
    ``ls DEFAULT_SLOT_DIR | wc -l`` = shared pool size, so mixing in
    uploader-priority-pool holds would let ``held`` exceed ``TOTAL``
    and clamp ``free`` to zero even when the shared pool has headroom.

    ``holds_by_label`` MUST still cover both pools — a Case-2 uploader
    holding a priority slot should surface in its row's ``[Slots: 1]``
    readout regardless of whether the shared pool is saturated.
    """
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import _fetch_slot_snapshot

    # TOTAL 24. Three shared-pool-only count samples all say 22.
    # Final structured pass: shared-pool COUNT is 22 (median stays 22 →
    # held = 22, free = 2), but the HOLDER lines include a
    # priority-pool uploader whose slot is NOT in that 22.
    out = (
        "TOTAL 24\n"
        "22\n"
        "22\n"
        "22\n"
        "COUNT 22\n"
        "HOLDER nara+franklin-d-roosevelt-library\n"  # priority-pool uploader
        "HOLDER nara+jimmy-carter-library\n"
        "HOLDER nara+jimmy-carter-library\n"
        "HOLDER nara+jimmy-carter-library\n"
        "HOLDER nara+jimmy-carter-library\n"
        "HOLDER nara+jimmy-carter-library\n"
        "HOLDER nara+jimmy-carter-library\n"
    )
    with patch("scripts.wikimedia_upload_status.ssm_run", return_value=out):
        snap = _fetch_slot_snapshot(object())
    assert snap is not None
    # Aggregate is shared-pool only: 22 held of 24 → 2 free.
    assert snap.line == "Worker slots: ~2 free of 24 (22 held)"
    assert snap.free == 2
    # But per-session attribution still includes the priority-pool holder.
    assert snap.holds_by_label == {
        "nara+franklin-d-roosevelt-library": 1,
        "nara+jimmy-carter-library": 6,
    }


def test_fetch_slot_snapshot_tolerates_no_holders():
    """Some polls will legitimately catch a moment with zero holders
    (transient slack). ``holds_by_label`` is empty and the aggregate
    still renders."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import _fetch_slot_snapshot

    out = "TOTAL 24\n0\n0\n0\nCOUNT 0\n"
    with patch("scripts.wikimedia_upload_status.ssm_run", return_value=out):
        snap = _fetch_slot_snapshot(object())
    assert snap is not None
    assert snap.line == "Worker slots: ~24 free of 24 (0 held)"
    assert snap.free == 24
    assert snap.holds_by_label == {}


def test_slot_suffix_appends_slots_when_session_holds_slots():
    """Under saturation, a slot-consuming row that holds N slots gets a
    ``[Slots: N]`` suffix so the reader can attribute pool usage
    without SSM-ing in."""
    from scripts.wikimedia_upload_status import _slot_suffix_for_row

    suffix = _slot_suffix_for_row(
        "nara+jimmy-carter-library",
        "SDC syncing (1,359 / 11,011 files, ~12.3%)",
        {"nara+jimmy-carter-library": 4, "ohio+state-library-of-ohio": 1},
    )
    assert suffix == " [Slots: 4]"


def test_slot_suffix_marks_awaiting_when_slot_phase_holds_zero():
    """Under saturation, a session that IS in a slot-consuming phase
    but appears in zero holders gets ``[Awaiting slot]`` — every worker
    in that session is blocked on acquire (or transiently between
    items, which under 0-free-slot conditions is dominated by the
    acquire wait)."""
    from scripts.wikimedia_upload_status import _slot_suffix_for_row

    suffix = _slot_suffix_for_row(
        "ohio+toledo-lucas-county-public-library",
        "Uploading (10,747 / 142,392 files, ~7.5%)",
        {"nara+franklin-d-roosevelt-library": 1},
    )
    assert suffix == " [Awaiting slot]"


def test_slot_suffix_strips_batch_suffix_before_lookup():
    """Regression (CR flagged on PR #366): multi-target sessions render
    with a ``[n/m]`` batch-position annotation on their display id
    (via ``_with_batch_suffix``), but ``WIKIMEDIA_SESSION_LABEL`` — the
    key in ``holds_by_label`` — carries only the raw slug. The suffix
    helper must strip the ``[n/m]`` before lookup, otherwise every
    multi-target row would miss its own hold count and render
    ``[Awaiting slot]`` while its workers are actively uploading.
    """
    from scripts.wikimedia_upload_status import _slot_suffix_for_row

    # Batch-annotated display_id, holds are keyed by the raw label.
    suffix = _slot_suffix_for_row(
        "texas+baylor-county-free-library [17/54]",
        "SDC syncing (11 / 4,452 files, ~0.2%)",
        {"texas+baylor-county-free-library": 6},
    )
    assert suffix == " [Slots: 6]"


def test_slot_suffix_suppressed_when_phase_already_shows_waiting_marker():
    """A phase text that already ends with the ``⏸ waiting on slots``
    marker (appended by ``get_phase_and_progress`` when the session's
    last log line is ``SLOTS_BUSY_LOG_MARKER``) must NOT ALSO get a
    ``[Awaiting slot]`` suffix. The two indicators mean the same
    thing; rendering both produces phrasing like
    ``Uploading (…) ⏸ waiting on slots [Awaiting slot]`` which reads
    like a compound state. When the log-tail marker is already there
    we defer to it and add nothing.
    """
    from scripts.wikimedia_upload_status import _slot_suffix_for_row

    suffix = _slot_suffix_for_row(
        "ohio+state-library-of-ohio",
        "Uploading (75 / 17,349 files, ~0.4%) ⏸ waiting on slots",
        {},  # zero holds for this session
    )
    assert suffix == ""


def test_slot_suffix_empty_for_non_slot_phase():
    """A session in a phase that doesn't touch the slot pool
    (downloading, generating IDs, complete, error) gets NO slot
    suffix — the pool line is irrelevant to it."""
    from scripts.wikimedia_upload_status import _slot_suffix_for_row

    for phase in (
        "Downloading (500 / 1000 items, ~50.0%)",
        "Generating IDs",
        "Upload complete (1200 items)",
        "Unknown (error)",
        "Starting...",
    ):
        assert _slot_suffix_for_row("anything", phase, {}) == "", phase


# ---------------------------------------------------------------------------
# find_active_label — single-SSM-call refactor of the per-label walk


def test_find_active_label_picks_freshest_log_across_labels():
    """Returns the label whose log was most recently modified. The single
    SSM call returns ``<mtime> <filename>`` for the freshest match; the
    helper parses out the label."""
    from unittest.mock import patch

    from ingest_wikimedia.session_state import find_active_label

    # Latest-mtime log file in the partner dir is the upload log for
    # bpl+phillips-academy. The helper should pick that label.
    with patch(
        "ingest_wikimedia.session_state.ssm_run",
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

    from ingest_wikimedia.session_state import find_active_label

    with patch("ingest_wikimedia.session_state.ssm_run", return_value=""):
        result = find_active_label(client=None, labels=["bpl+phillips-academy"])
    assert result is None


def test_find_active_label_returns_none_for_empty_label_list():
    """Defensive: empty label list short-circuits without an SSM round
    trip. The helper is reached only via `parse_session_labels`, which
    can return an empty list on a malformed session name."""
    from unittest.mock import patch

    from ingest_wikimedia.session_state import find_active_label

    with patch("ingest_wikimedia.session_state.ssm_run") as ssm:
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

    from ingest_wikimedia.session_state import find_active_label

    captured: list[str] = []

    def fake_ssm_run(_client, command, **_kwargs):
        captured.append(command)
        return "1700000000.0 20260528-091047-bpl+phillips-academy-upload.log"

    with patch("ingest_wikimedia.session_state.ssm_run", side_effect=fake_ssm_run):
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

    from ingest_wikimedia.session_state import find_active_label

    # ``find ... | sort -rn | head -1`` returns the highest-mtime line;
    # we just have to feed it that one line. The active CFLA SDC log
    # would be ranked above the aborted Clinton SDC log because its
    # mtime is later.
    with patch(
        "ingest_wikimedia.session_state.ssm_run",
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


def test_find_active_label_bounds_lookup_by_session_created():
    """Regression: two concurrent sessions can each contain the same
    label in their chain — e.g. a 54-target texas chain and a 3-target
    fixup chain that overlap on ``texas+harrie-p-woodson-memorial-library``.
    When the smaller chain writes to that label's log, the log's mtime
    becomes the freshest across the big chain's label set too, and the
    big chain gets misreported as "active on harrie" even though it
    completed harrie hours ago and is really further down its own
    chain.

    Fix: pass the tmux session's creation epoch to ``find_active_label``
    which appends ``-newermt "@N"`` to the ``find`` command, bounding
    the lookup to files created after this session's tmux window
    began. A completed-target log written by a concurrent session gets
    filtered out.
    """
    from unittest.mock import patch

    from ingest_wikimedia.session_state import find_active_label

    captured: list[str] = []

    def fake_ssm_run(_client, command, **_kwargs):
        captured.append(command)
        return "1700005000.0 20260528-091047-bpl+phillips-academy-upload.log"

    with patch("ingest_wikimedia.session_state.ssm_run", side_effect=fake_ssm_run):
        find_active_label(
            client=None,
            labels=["bpl+phillips-academy"],
            session_created=1700000000,
        )

    assert len(captured) == 1
    assert "-newermt '@1700000000'" in captured[0], (
        f"``find`` command must include a ``-newermt '@<epoch>'`` filter "
        f"when session_created is set; got: {captured[0]!r}"
    )


def test_find_active_label_no_session_bound_when_created_is_zero():
    """The ``-newermt`` filter must be OMITTED when ``session_created=0``
    (default). This preserves backwards compatibility for callers that
    don't yet thread the tmux session-creation epoch through — and
    matches the semantics of "no bound".
    """
    from unittest.mock import patch

    from ingest_wikimedia.session_state import find_active_label

    captured: list[str] = []

    def fake_ssm_run(_client, command, **_kwargs):
        captured.append(command)
        return "1700005000.0 20260528-091047-bpl+phillips-academy-upload.log"

    with patch("ingest_wikimedia.session_state.ssm_run", side_effect=fake_ssm_run):
        find_active_label(client=None, labels=["bpl+phillips-academy"])

    assert len(captured) == 1
    assert "-newermt" not in captured[0], (
        f"``find`` command must NOT include ``-newermt`` when "
        f"session_created is 0; got: {captured[0]!r}"
    )


def test_find_active_label_matches_drain_hub_log():
    """Regression: a session in its terminal partner-level drain phase
    writes ``…-drain-<hub>-drain-deferred.log``. That label is NOT in
    the session's per-target label list, but the reporter needs to see
    it — previously such a session showed ``SDC complete`` while the
    tmux window was busy on the box-wide drain flock (PR bug 2).

    ``find_active_label`` synthesizes ``drain-<hub>`` alternatives for
    each unique hub in the input label set so the freshest drain log
    surfaces correctly. On match the synthetic label is returned so
    downstream callers can dispatch on it.
    """
    from unittest.mock import patch

    from ingest_wikimedia.session_state import find_active_label

    with patch(
        "ingest_wikimedia.session_state.ssm_run",
        return_value=("1700009000.0 20260705-092303-drain-ohio-drain-deferred.log"),
    ):
        result = find_active_label(
            client=None,
            labels=["ohio+ohio-university-libraries"],
        )
    assert result is not None
    label, _ = result
    assert label == "drain-ohio", (
        f"drain-hub synthetic label must be returned when the freshest "
        f"log is a drain-<hub> file; got {label!r}"
    )


def test_find_active_label_rejects_filename_not_in_label_list():
    """Defensive: if the ``find`` regex's alternation somehow returns a
    label that isn't in ``labels`` (regex prefix collision in a edge
    case), the helper returns ``None`` rather than reporting a stray
    label."""
    from unittest.mock import patch

    from ingest_wikimedia.session_state import find_active_label

    with patch(
        "ingest_wikimedia.session_state.ssm_run",
        return_value="1700000000.0 20260528-091047-some+other-hub-upload.log",
    ):
        result = find_active_label(client=None, labels=["bpl+phillips-academy"])
    assert result is None


def test_snapshot_running_active_labels_parses_multiple_sessions():
    """Ground-truth signal for active-label detection: one SSM
    roundtrip parses tmux+ps+/proc/<pid>/environ output into
    ``{session_name: label}``. Each pipe-delimited line comes from a
    running child of a distinct tmux session's bash pipeline; the
    label field is that child's ``WIKIMEDIA_SESSION_LABEL`` env var.
    """
    from unittest.mock import patch

    from ingest_wikimedia.session_state import snapshot_running_active_labels

    with patch(
        "ingest_wikimedia.session_state.ssm_run",
        return_value=(
            "wikimedia-texas+livingston-and-53-more"
            "|texas+botanical-research-institute-of-texas\n"
            "wikimedia-texas+montgomery-plus-2"
            "|texas+harrie-p-woodson-memorial-library\n"
            "wikimedia-ohio-and-3-more|drain-ohio\n"
        ),
    ):
        snap = snapshot_running_active_labels(client=None)
    assert snap == {
        "wikimedia-texas+livingston-and-53-more": (
            "texas+botanical-research-institute-of-texas"
        ),
        "wikimedia-texas+montgomery-plus-2": (
            "texas+harrie-p-woodson-memorial-library"
        ),
        "wikimedia-ohio-and-3-more": "drain-ohio",
    }


def test_snapshot_running_active_labels_disambiguates_shared_labels():
    """The core defect the subprocess signal fixes: two concurrent
    sessions have the SAME target label (e.g. both texas chains include
    harrie-p-woodson). The mtime heuristic can't distinguish which
    session's subprocess is writing the log right now, so it mis-
    attributes. The subprocess signal (reading each session's running
    child's ``WIKIMEDIA_SESSION_LABEL``) resolves that unambiguously.
    """
    from unittest.mock import patch

    from ingest_wikimedia.session_state import snapshot_running_active_labels

    # Big chain is on 'botanical', small chain is on 'harrie-p-woodson'.
    # A pure-mtime heuristic would have picked harrie-p-woodson for
    # BOTH sessions because the small chain is writing that log right
    # now (highest mtime across the big chain's label set too).
    with patch(
        "ingest_wikimedia.session_state.ssm_run",
        return_value=(
            "wikimedia-texas+livingston-and-53-more"
            "|texas+botanical-research-institute-of-texas\n"
            "wikimedia-texas+montgomery-plus-2"
            "|texas+harrie-p-woodson-memorial-library\n"
        ),
    ):
        snap = snapshot_running_active_labels(client=None)
    assert (
        snap["wikimedia-texas+livingston-and-53-more"]
        == "texas+botanical-research-institute-of-texas"
    )
    assert (
        snap["wikimedia-texas+montgomery-plus-2"]
        == "texas+harrie-p-woodson-memorial-library"
    )


def test_snapshot_running_active_labels_empty_output():
    """No sessions with a running child (all sessions in id-generation
    cold start, or no sessions at all) → empty dict, callers fall back
    to :func:`find_active_label`."""
    from unittest.mock import patch

    from ingest_wikimedia.session_state import snapshot_running_active_labels

    with patch("ingest_wikimedia.session_state.ssm_run", return_value=""):
        assert snapshot_running_active_labels(client=None) == {}


def test_active_and_upcoming_labels_uses_provided_active_label_over_mtime():
    """Passing a pre-resolved ``active_label`` skips the mtime lookup —
    load-bearing property: callers pre-fetch the snapshot once, then
    invoke this helper per-session as a pure decision over the
    resolved label."""
    from unittest.mock import patch

    from ingest_wikimedia.session_state import active_and_upcoming_labels

    labels = ["ohio+a", "ohio+b", "ohio+c"]
    with patch(
        "ingest_wikimedia.session_state.find_active_label",
        side_effect=AssertionError("mtime path must not run"),
    ) as mtime:
        upcoming = active_and_upcoming_labels(
            ssm=None,
            labels=labels,
            session_created=1700000000,
            active_label="ohio+b",
        )
    mtime.assert_not_called()
    assert upcoming == {"ohio+b", "ohio+c"}


def test_active_and_upcoming_labels_falls_back_to_mtime_when_active_label_none():
    """No pre-resolved active_label → fall through to the mtime
    heuristic (id-generation cold start, between steps, chain
    finished)."""
    from unittest.mock import patch

    from ingest_wikimedia.session_state import active_and_upcoming_labels

    labels = ["ohio+a", "ohio+b", "ohio+c"]
    with patch(
        "ingest_wikimedia.session_state.find_active_label",
        return_value=("ohio+c", 1700009999),
    ) as mtime:
        upcoming = active_and_upcoming_labels(
            ssm=None,
            labels=labels,
            session_created=1700000000,
            active_label=None,
        )
    mtime.assert_called_once()
    assert upcoming == {"ohio+c"}


def test_active_and_upcoming_labels_empty_set_for_drain_hub_active_label():
    """``drain-<hub>`` means the terminal partner-level drain phase —
    all per-target work is done → empty set so a new request for any
    per-target label is not a conflict."""
    from ingest_wikimedia.session_state import active_and_upcoming_labels

    upcoming = active_and_upcoming_labels(
        ssm=None,
        labels=["ohio+a", "nwh+b"],
        active_label="drain-ohio",
    )
    assert upcoming == set()


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


def test_main_uses_subprocess_snapshot_over_mtime_for_active_label():
    """When ``snapshot_running_active_labels`` returns an active label
    for a session, ``fetch`` must use it — NOT fall through to
    :func:`find_active_label`'s log-mtime heuristic. This is the fix
    for the two-sessions-share-a-label bug: mtime can't distinguish
    which session's subprocess is writing a shared log, but the
    ``WIKIMEDIA_SESSION_LABEL`` env var of each session's running
    child does.
    """
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import main

    sessions_out = "wikimedia-texas+livingston-and-1-more|1700000000\n"
    captured, fake_post = _capture_slack_post()

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
            "scripts.wikimedia_upload_status._fetch_slot_snapshot",
            return_value=None,
        ),
        patch(
            "scripts.wikimedia_upload_status.parse_session_labels",
            return_value=[
                "texas+livingston-municipal-library",
                "texas+botanical-research-institute-of-texas",
            ],
        ),
        patch(
            "scripts.wikimedia_upload_status.snapshot_running_active_labels",
            return_value={
                "wikimedia-texas+livingston-and-1-more": (
                    "texas+botanical-research-institute-of-texas"
                ),
            },
        ),
        patch(
            "scripts.wikimedia_upload_status.find_active_label",
            side_effect=AssertionError(
                "find_active_label (mtime) must not be consulted when "
                "the subprocess snapshot has a label for the session"
            ),
        ),
        patch(
            "scripts.wikimedia_upload_status.get_phase_and_progress",
            return_value=("Uploading (100 / 500 files, ~20.0%)", 1700009999),
        ),
        patch(
            "scripts.wikimedia_upload_status.requests.post",
            side_effect=fake_post,
        ),
    ):
        main()

    payload = captured["payload"]
    section_blocks = [b for b in payload["blocks"] if b.get("type") == "section"]
    text = section_blocks[0]["text"]["text"]
    # The subprocess signal (botanical) wins over any mtime latch onto
    # a shared label. Row is positioned [2/2] because botanical is
    # position 2 of the batch.
    assert "texas+botanical-research-institute-of-texas [2/2]" in text
    assert "Uploading (100 / 500 files, ~20.0%)" in text


def test_main_drain_hub_row_attributes_to_parent_chain():
    """Terminal drain phase renders as one row for the whole batch:
    label is the batch identity (no drain-<hub> annotation), and
    ``queue_hubs`` names every canonical partner so the phase reader
    can sum sidecars across the batch rather than showing only the
    currently-active partner's slice."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import main

    sessions_out = "wikimedia-ohio+ohio-uni-and-3-more|1700000000\n"
    captured, fake_post = _capture_slack_post()
    captured_calls: list[dict] = []

    def fake_phase(client, session, hub, label, **kw):
        captured_calls.append({"hub": hub, "label": label, **kw})
        return (
            "Draining (13 queued, Category:Duplicate at 1,004, needs < 900)",
            1700009999,
        )

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
            "scripts.wikimedia_upload_status._fetch_slot_snapshot",
            return_value=None,
        ),
        patch(
            "scripts.wikimedia_upload_status.parse_session_labels",
            return_value=[
                "ohio+ohio-university-libraries",
                "northwest-heritage+whitman-county-library",
                "minnesota+minnesota-legislative-reference-library",
                "bpl+phillips-academy",
            ],
        ),
        patch(
            "scripts.wikimedia_upload_status.snapshot_running_active_labels",
            return_value={"wikimedia-ohio+ohio-uni-and-3-more": "drain-ohio"},
        ),
        patch(
            "scripts.wikimedia_upload_status.get_phase_and_progress",
            side_effect=fake_phase,
        ),
        patch(
            "scripts.wikimedia_upload_status.requests.post",
            side_effect=fake_post,
        ),
    ):
        main()

    payload = captured["payload"]
    section_blocks = [b for b in payload["blocks"] if b.get("type") == "section"]
    text = section_blocks[0]["text"]["text"]
    assert "ohio+ohio-university-libraries +3 more" in text
    assert "(drain: ohio)" not in text
    assert "`drain-ohio`" not in text
    assert len(captured_calls) == 1
    assert set(captured_calls[0]["queue_hubs"]) == {
        "ohio",
        "northwest-heritage",
        "minnesota",
        "bpl",
    }


def test_main_non_drain_row_passes_no_queue_hubs():
    """Non-drain per-target rows pass ``queue_hubs=None`` so
    ``_drain_deferred_phase`` (if reached) sums only the row's own
    partner's sidecar — the batch-total behavior is scoped to the
    terminal drain phase."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import main

    captured_calls: list[dict] = []

    def fake_phase(client, session, hub, label, **kw):
        captured_calls.append({"hub": hub, "label": label, **kw})
        return ("Uploading (10 / 100, ~10%)", 1700000000)

    with (
        patch.dict(
            "os.environ",
            {"DPLA_SLACK_BOT_TOKEN": "tok", "NOTIFY_IF_IDLE": "false"},
        ),
        patch("scripts.wikimedia_upload_status.boto3.client", return_value=object()),
        patch(
            "scripts.wikimedia_upload_status.ssm_run",
            return_value="wikimedia-bpl+phillips-academy|1700000000\n",
        ),
        patch(
            "scripts.wikimedia_upload_status.fetch_memory_snapshot",
            return_value=None,
        ),
        patch(
            "scripts.wikimedia_upload_status._fetch_slot_snapshot",
            return_value=None,
        ),
        patch(
            "scripts.wikimedia_upload_status.snapshot_running_active_labels",
            return_value={"wikimedia-bpl+phillips-academy": "bpl+phillips-academy"},
        ),
        patch(
            "scripts.wikimedia_upload_status.get_phase_and_progress",
            side_effect=fake_phase,
        ),
        patch(
            "scripts.wikimedia_upload_status.requests.post",
            side_effect=_capture_slack_post()[1],
        ),
    ):
        main()

    assert captured_calls == [
        {"hub": "bpl", "label": "bpl+phillips-academy", "queue_hubs": None}
    ]


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

    sessions_out = "wikimedia-bpl+phillips-academy|1700000000\n"
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
            "scripts.wikimedia_upload_status._fetch_slot_snapshot",
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


def test_main_appends_slot_suffixes_when_pool_is_saturated():
    """CR nitpick on PR #366: prior ``main()`` tests all mock
    ``_fetch_slot_snapshot`` to ``None``, leaving the saturated-suffix
    wiring untested end-to-end. This exercise it: snapshot reports
    ``free == 0`` with a populated ``holds_by_label`` covering one
    session's SDC-sync worker pool + a second session that holds zero
    slots. Assertions verify that the final Slack section text carries
    ``[Slots: 4]`` and ``[Awaiting slot]`` on the appropriate rows.
    """
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import SlotSnapshot, main

    # ``tmux ls -F '#{session_name}|#{session_created}'`` output shape —
    # matches what wikimedia_upload_status now issues so
    # ``_parse_session_line`` extracts the creation epoch alongside the
    # session name.
    sessions_out = (
        "wikimedia-nara+jimmy-carter-library|1700000000\n"
        "wikimedia-ohio+state-library-of-ohio|1700000000\n"
    )
    captured, fake_post = _capture_slack_post()

    # Deterministic per-session phase results. jimmy-carter is
    # SDC-syncing with 4 workers currently holding slots (2 waiting).
    # state-library-of-ohio is an uploader that holds zero slots
    # (saturation blocked it before it could grab one).
    def fake_get_phase_and_progress(client, session, hub, label, **_kw):
        if label == "nara+jimmy-carter-library":
            return ("SDC syncing (1,359 / 11,011 files, ~12.3%)", 1700000000)
        if label == "ohio+state-library-of-ohio":
            return ("Uploading (75 / 17,349 files, ~0.4%)", 1700000000)
        return (None, 0)

    with (
        patch.dict(
            "os.environ",
            {"DPLA_SLACK_BOT_TOKEN": "tok", "NOTIFY_IF_IDLE": "false"},
        ),
        patch("scripts.wikimedia_upload_status.boto3.client", return_value=object()),
        patch("scripts.wikimedia_upload_status.ssm_run", return_value=sessions_out),
        patch(
            "scripts.wikimedia_upload_status.fetch_memory_snapshot",
            return_value=None,
        ),
        patch(
            "scripts.wikimedia_upload_status._fetch_slot_snapshot",
            return_value=SlotSnapshot(
                line="Worker slots: ~0 free of 24 (24 held)",
                free=0,
                holds_by_label={"nara+jimmy-carter-library": 4},
            ),
        ),
        patch(
            "scripts.wikimedia_upload_status.find_active_label",
            side_effect=lambda ssm, labels, **_kw: (labels[0], 1700000000),
        ),
        patch(
            "scripts.wikimedia_upload_status.get_phase_and_progress",
            side_effect=fake_get_phase_and_progress,
        ),
        patch(
            "scripts.wikimedia_upload_status.requests.post",
            side_effect=fake_post,
        ),
    ):
        main()

    payload = captured["payload"]
    section_text = "\n".join(
        b["text"]["text"] for b in payload["blocks"] if b.get("type") == "section"
    )

    # Each status row renders as a single line (``\`display_id\` phase{suffix}``),
    # so locate each session's own row and assert the suffix is attached to *that*
    # row — not merely present somewhere in the concatenated block. This pins
    # attribution: swapping the suffixes between rows must fail the test.
    def _row_for(display_id: str) -> str:
        matches = [ln for ln in section_text.splitlines() if display_id in ln]
        assert len(matches) == 1, (
            f"expected exactly one row for {display_id!r}, got {matches!r}"
        )
        return matches[0]

    # jimmy-carter holds 4 slots → [Slots: 4] on its own row, and it must not
    # be the one flagged as awaiting.
    jimmy_row = _row_for("nara+jimmy-carter-library")
    assert "[Slots: 4]" in jimmy_row, jimmy_row
    assert "[Awaiting slot]" not in jimmy_row, jimmy_row
    # state-library-of-ohio is in an upload phase but holds zero → [Awaiting slot]
    # on its own row, and it must not claim any held slots.
    ohio_row = _row_for("ohio+state-library-of-ohio")
    assert "[Awaiting slot]" in ohio_row, ohio_row
    assert "[Slots:" not in ohio_row, ohio_row


def test_fetch_slot_snapshot_returns_none_on_malformed_output():
    """CR nitpick on PR #366: the parse block must degrade to ``None``
    rather than letting a malformed ``TOTAL``/``COUNT`` line propagate
    out of ``slots_future.result()`` and abort the whole status post.
    Matches :func:`fetch_memory_snapshot`'s own graceful-degradation
    contract — the slot line is optional context, not load-bearing.
    """
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import _fetch_slot_snapshot

    # ``TOTAL`` line with a non-integer value → ``int(...)`` raises;
    # helper must swallow and return ``None``.
    with patch(
        "scripts.wikimedia_upload_status.ssm_run",
        return_value="TOTAL nope\n1\n1\n1\nCOUNT 1\n",
    ):
        assert _fetch_slot_snapshot(object()) is None


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


def test_total_ordinals_awk_prefers_item_summary_with_downloading_fallback():
    """The total-ordinals denominator sums the per-item ``Item <id>: N
    ordinals`` summary (downloader.py:563), which the downloader emits for
    EVERY item regardless of whether its media was fetched or already-staged/
    skipped. It falls back to counting the per-ordinal ``Downloading <partner>
    <id> <ordinal> from <url>`` line ONLY when the Item-summary sum is 0 — i.e.
    pre-PR-272 logs that lack the summary.

    Counting ``Downloading`` alone (the previous contract) collapsed to 0 for
    any already-staged run — re-runs, SDC-only relaunches, and download-once-
    then-iterate hubs like NARA — because that line fires only on an actual
    fetch attempt, which wrongly dropped the row from file- to
    item-granularity. Pin the rendered SSM command: both patterns present,
    Item-sum preferred."""
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
    # Primary: sum the per-item Item-summary (present for every item, even an
    # already-staged/skipped run that logged no Downloading lines).
    assert "Item [a-f0-9]+: [0-9]+ ordinals" in main_cmd, main_cmd
    # Fallback for pre-#272 logs that lack the summary.
    assert "Downloading [a-z0-9-]+ [a-f0-9]+ [0-9]+ from" in main_cmd, main_cmd
    # END prefers the Item-sum, using the Downloading count only when it's 0.
    assert "item>0 ? item : dl" in main_cmd, main_cmd


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

    sessions_out = "wikimedia-texas+a+texas+b+texas+c+texas+d|1700000000\n"
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
        patch(
            "scripts.wikimedia_upload_status._fetch_slot_snapshot", return_value=None
        ),
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

    sessions_out = "wikimedia-bpl+phillips-academy|1700000000\n"
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
        patch(
            "scripts.wikimedia_upload_status._fetch_slot_snapshot", return_value=None
        ),
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


def test_get_phase_and_progress_reports_generating_ids():
    """A session whose newest recognized log is an ``-id-generation.log``
    surfaces as ``Generating IDs (N items enumerated)`` — get-ids-es writes
    that log with per-page progress. Previously id-generation produced no
    recognized log and the session was misclassified."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    calls = []

    def fake_ssm_run(_client, _command, **_kwargs):
        calls.append(_command)
        if len(calls) == 1:  # precheck: session_created + newest matching log
            return (
                "1700000000\n"
                "20260709-120000-bpl+boston-public-library-id-generation.log\n"
            )
        # id-generation branch: stat mtime + latest "N items enumerated" line
        return "1700000000\n42,345 items enumerated\n"

    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake_ssm_run):
        phase, _ = get_phase_and_progress(
            client=None,
            session="wikimedia-bpl+boston-public-library",
            hub="bpl",
            label="bpl+boston-public-library",
        )
    assert phase == "Generating IDs (42,345 items enumerated)", phase


def test_institution_label_no_log_does_not_grab_partner_drain_log():
    """An institution session in id-generation (no matching log yet) must NOT
    fall back to the hub-slug glob and pick up an unrelated partner-level
    ``drain-<hub>`` log — that misreported id-gen sessions as ``Draining``. It
    returns None so the caller renders ``Generating IDs``."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    calls = []

    def fake_ssm_run(_client, _command, **_kwargs):
        calls.append(_command)
        if len(calls) == 1:  # precheck: session_created + NO matching log
            return "1700000000\n"
        # A buggy hub-slug fallback WOULD return this partner-drain log.
        return "20260709-143326-drain-bpl-drain-deferred.log\n"

    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake_ssm_run):
        phase, _ = get_phase_and_progress(
            client=None,
            session="wikimedia-bpl+boston-public-library",
            hub="bpl",
            label="bpl+boston-public-library",
        )
    assert phase is None, phase  # caller (main) renders this as "Generating IDs"
    assert len(calls) == 1, "must not run the hub-slug fallback query for a '+' label"


def test_drain_deferred_phase_recognizes_empty_sidecar_completion():
    """A drain that found an empty/missing sidecar logs ``nothing to do.`` and
    exits without acquiring the lock — it must read as complete, not
    ``⏸ waiting for host lock`` (the pre-fix default when no lock-acquired
    marker was present)."""
    from unittest.mock import patch

    from scripts.wikimedia_upload_status import get_phase_and_progress

    sep = "__WM_DRAIN_SEP__"  # _drain_deferred_phase's own separator
    calls = []

    def fake_ssm_run(_client, _command, **_kwargs):
        calls.append(_command)
        if len(calls) == 1:  # precheck
            return "1700000000\n20260709-143326-drain-bpl-drain-deferred.log\n"
        # _drain_deferred_phase sections: mtime, tail, queued, lock_acquired
        tail = (
            "Drain-deferred: sidecar for partner bpl is empty (or missing); "
            "nothing to do."
        )
        return f"1700000000\n{sep}\n{tail}\n{sep}\n0\n{sep}\n0"

    with patch("scripts.wikimedia_upload_status.ssm_run", side_effect=fake_ssm_run):
        phase, _ = get_phase_and_progress(
            client=None,
            session="wikimedia-drain-bpl",
            hub="bpl",
            label="drain-bpl",
        )
    assert phase == "Drain complete (nothing queued)", phase
