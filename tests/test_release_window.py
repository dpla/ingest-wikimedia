"""Tests for the pure logic of metrics/dpla-dup-window/release_window.py.

The module lives in a hyphenated directory (not an importable package) and
imports pywikibot at module load, so it's loaded here by file path. Only the
pure, site-free functions are exercised — parse/render of the window value and
the release-plan arithmetic. The pywikibot wiring (login, category reads,
purge) is integration-tested against a live site via --dry-run, not here.
"""

import importlib.util
import sys
from pathlib import Path

_MODULE_PATH = (
    Path(__file__).resolve().parent.parent
    / "metrics"
    / "dpla-dup-window"
    / "release_window.py"
)
_spec = importlib.util.spec_from_file_location("release_window", _MODULE_PATH)
release_window = importlib.util.module_from_spec(_spec)
# Register before exec: the module's frozen dataclass with a ``list[int]``
# field resolves its type via ``sys.modules[cls.__module__]`` at class-creation
# time (compounded by ``from __future__ import annotations``), which fails if
# the module isn't yet registered.
sys.modules["release_window"] = release_window
_spec.loader.exec_module(release_window)

parse_window_value = release_window.parse_window_value
render_window_value = release_window.render_window_value
compute_release_plan = release_window.compute_release_plan
is_dpla_duplicate_title = release_window.is_dpla_duplicate_title


# --------------------------------------------------------------------------
# is_dpla_duplicate_title — the mechanism-agnostic DPLA-file signal
# --------------------------------------------------------------------------


def test_is_dpla_title_matches_dpla_filename():
    assert is_dpla_duplicate_title(
        "Letter to Riley - DPLA - 8c61a9e566718f471a8ec9666fc31f45 (page 2).jpg"
    )


def test_is_dpla_title_rejects_non_dpla():
    # A community-uploaded duplicate sharing Category:Duplicate must not count
    # against the DPLA budget.
    assert not is_dpla_duplicate_title("Some Community Photo.jpg")
    # "DPLA" without the spaced token (e.g. an org name in prose) must not match.
    assert not is_dpla_duplicate_title("History of DPLA the archive.png")


def test_is_dpla_title_is_mechanism_agnostic():
    # Both a template-released file and a bot plain-{{Duplicate}} file share the
    # same filename token, so both count toward the shared budget.
    template_released = "X - DPLA - abcdef1234567890abcdef1234567890 (page 1).jpg"
    bot_plain_tagged = "Y - DPLA - 0123456789abcdef0123456789abcdef.jpg"
    assert is_dpla_duplicate_title(template_released)
    assert is_dpla_duplicate_title(bot_plain_tagged)


# --------------------------------------------------------------------------
# parse_window_value — fail-closed to 0
# --------------------------------------------------------------------------


def test_parse_window_value_reads_includeonly_integer():
    text = "<includeonly>1234567890</includeonly><noinclude>docs</noinclude>"
    assert parse_window_value(text) == 1234567890


def test_parse_window_value_tolerates_whitespace_in_block():
    text = "<includeonly>\n  42\n</includeonly><noinclude>x</noinclude>"
    assert parse_window_value(text) == 42


def test_parse_window_value_missing_page_is_zero():
    # None / empty → fail closed (nothing released).
    assert parse_window_value(None) == 0
    assert parse_window_value("") == 0


def test_parse_window_value_bare_integer_body():
    # A hand-edited page with no includeonly wrapper still parses.
    assert parse_window_value("500") == 500


def test_parse_window_value_non_numeric_fails_closed():
    # Garbage / prose with no parseable value must not release everything.
    assert parse_window_value("<includeonly>not a number</includeonly>") == 0
    assert parse_window_value("hello world") == 0


def test_render_then_parse_roundtrips():
    for v in (0, 1, 99999, 20240101000000000):
        assert parse_window_value(render_window_value(v)) == v


def test_render_window_value_transcludes_only_the_integer():
    # The digits must sit inside <includeonly> so transclusion yields exactly
    # the number, and the human note must be inside <noinclude>.
    out = render_window_value(777)
    assert "<includeonly>777</includeonly>" in out
    assert "<noinclude>" in out and "</noinclude>" in out
    # Note text lives only in the noinclude section.
    assert out.index("777") < out.index("<noinclude>")


# --------------------------------------------------------------------------
# compute_release_plan — the arithmetic core
# --------------------------------------------------------------------------


def test_plan_noop_when_at_target():
    plan = compute_release_plan(
        current_window=100,
        visible_count=100,
        target=100,
        unrevealed_keys_ascending=[101, 102],
    )
    assert plan.is_noop
    assert plan.new_window == 100
    assert plan.keys_to_reveal == []


def test_plan_noop_when_above_target():
    plan = compute_release_plan(
        current_window=100,
        visible_count=130,
        target=100,
        unrevealed_keys_ascending=[200],
    )
    assert plan.is_noop


def test_plan_releases_exactly_the_deficit():
    # Visible 60, target 100 → release the 40 oldest unrevealed.
    keys = list(range(1000, 1200))  # 200 unrevealed, ascending
    plan = compute_release_plan(
        current_window=1000,
        visible_count=60,
        target=100,
        unrevealed_keys_ascending=keys,
    )
    assert len(plan.keys_to_reveal) == 40
    assert plan.keys_to_reveal == keys[:40]
    # Window sits just past the 40th key so exactly those release.
    assert plan.new_window == keys[39] + 1


def test_plan_window_reveals_precisely_taken_keys():
    # Every taken key is strictly below new_window; the next key is not.
    keys = [10, 20, 30, 40, 50]
    plan = compute_release_plan(
        current_window=10, visible_count=0, target=3, unrevealed_keys_ascending=keys
    )
    assert plan.keys_to_reveal == [10, 20, 30]
    assert plan.new_window == 31
    # 10,20,30 < 31 (released); 40,50 >= 31 (still backlog).
    assert all(k < plan.new_window for k in [10, 20, 30])
    assert all(k >= plan.new_window for k in [40, 50])


def test_plan_fewer_backlog_files_than_deficit_takes_all():
    # Backlog nearly drained: only 5 left but deficit is 40.
    keys = [7, 8, 9, 10, 11]
    plan = compute_release_plan(
        current_window=7, visible_count=60, target=100, unrevealed_keys_ascending=keys
    )
    assert plan.keys_to_reveal == keys
    assert plan.new_window == 12


def test_plan_noop_when_no_unrevealed_files():
    plan = compute_release_plan(
        current_window=500, visible_count=10, target=100, unrevealed_keys_ascending=[]
    )
    assert plan.is_noop
    assert plan.new_window == 500


def test_plan_is_monotonic_never_regresses_window():
    # Defensive: even if handed keys below the current window (out of
    # contract), the window must not move backward.
    plan = compute_release_plan(
        current_window=1000,
        visible_count=0,
        target=2,
        unrevealed_keys_ascending=[10, 20],
    )
    # candidate would be 21 (< 1000); guard keeps window at 1000 and releases
    # nothing (no forward progress possible from stale keys).
    assert plan.new_window == 1000
    assert plan.is_noop


def test_plan_first_fill_from_zero_window():
    # Initial state: window 0, nothing visible, full backlog. Release the
    # oldest `target` files.
    keys = list(range(1, 501))
    plan = compute_release_plan(
        current_window=0, visible_count=0, target=100, unrevealed_keys_ascending=keys
    )
    assert len(plan.keys_to_reveal) == 100
    assert plan.new_window == 100 + 1  # keys[99] == 100
