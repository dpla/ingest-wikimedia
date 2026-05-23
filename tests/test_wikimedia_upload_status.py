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
    """Only -download.log and -upload.log are valid phase logs."""
    from scripts.wikimedia_upload_status import log_filename_pattern_for_label

    pattern = re.compile(log_filename_pattern_for_label("nara"))
    assert pattern.search("20260522-100000-nara-download.log")
    assert pattern.search("20260522-100000-nara-upload.log")
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
