"""Focused tests for scripts/wikimedia_launch.py.

The launcher is a thin orchestrator over boto3 + SSM + Slack — most of its
behavior is exercised end-to-end by the GH Action that fires it. These
tests cover the argument-validation surface added in PR 5b/5c so a typo
in `--sdc-only` handling can't ship silently.
"""

from unittest.mock import patch

import pytest


def _run_main(argv: list[str]) -> SystemExit | None:
    """Invoke `wikimedia_launch.main()` with a synthetic argv and return
    the SystemExit raised by `_slack_fail` (or None if main exited cleanly).

    `_slack_fail` exits 1 after stderr + optional Slack notification; the
    test asserts on the SystemExit instance's `.code`.
    """
    import scripts.wikimedia_launch as launch_mod

    with patch("sys.argv", ["wikimedia_launch.py", *argv]):
        try:
            launch_mod.main()
        except SystemExit as e:
            return e
    return None


def test_refresh_only_and_sdc_only_are_mutually_exclusive():
    """Passing both --refresh-only and --sdc-only must fail fast with a
    clear error — they're distinct run modes and combining them silently
    would pick whichever branch the launcher tested first."""
    exit_info = _run_main(
        [
            "--partner",
            "minnesota",
            "--refresh-only",
            "true",
            "--sdc-only",
            "true",
        ]
    )
    assert exit_info is not None, "Expected SystemExit on conflicting flags"
    assert exit_info.code == 1


def test_sdc_only_alone_is_accepted_at_parse_time(monkeypatch, capsys):
    """`--sdc-only true` on its own must parse without error. We can't
    exercise the full launch chain in a unit test (it shells to EC2), so
    we just confirm parse_args + the mutual-exclusion gate let it
    through to the next step.

    Stub `ssm_run` so the SystemExit comes from a deterministic post-parse
    point (`_slack_fail("Failed to heal EC2 file ownership: …")`), not
    from an implicit AWS-credential failure that depends on whether the
    test runner happens to have creds. The assertion that matters is just
    that the exit is NOT the mutual-exclusion message — any post-parse
    exit point satisfies it.
    """
    monkeypatch.setattr(
        "scripts.wikimedia_launch.boto3.client",
        lambda *a, **kw: object(),
    )
    monkeypatch.setattr(
        "scripts.wikimedia_launch.ssm_run",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("ssm stubbed")),
    )
    exit_info = _run_main(
        [
            "--partner",
            "this-is-not-a-real-hub-slug",
            "--sdc-only",
            "true",
        ]
    )
    assert exit_info is not None
    # Anything that exits with code 1 is fine — the assertion that matters
    # is that the SystemExit is NOT the mutual-exclusion message (which
    # would mean we hit the wrong gate).
    captured = capsys.readouterr()
    assert "Cannot combine --refresh-only and --sdc-only" not in captured.err


@pytest.mark.parametrize(
    "value,expected",
    [
        ("true", True),
        ("True", True),
        ("TRUE", True),
        ("false", False),
        ("", False),
        # Anything that isn't a case-insensitive "true" match falls
        # through to False. Same convention for --force, --refresh-only,
        # and --sdc-only.
        ("yes", False),
        ("1", False),
    ],
)
def test_parse_bool(value, expected):
    """Exercise the launcher's actual boolean-string parser (the same
    helper that converts `args.sdc_only`, `args.refresh_only`, and
    `args.force` into bools). This locks in the case-insensitive "true"
    contract so a refactor of the parsing code can't silently flip the
    polarity."""
    from scripts.wikimedia_launch import _parse_bool

    assert _parse_bool(value) is expected
