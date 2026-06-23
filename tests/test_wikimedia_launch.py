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


# ---------------------------------------------------------------------------
# _slack_fail: ephemeral-by-default with operational fallback
# ---------------------------------------------------------------------------


def _raise_connect_timeout(*_args, **_kwargs):
    raise Exception("connect timeout")


def test_slack_fail_user_error_stays_ephemeral_only(monkeypatch):
    """Default `_slack_fail` call (no operational flag) goes ONLY to the
    slash command's response_url. User-error failures must stay private
    between the runner and the user; the public `#tech-alerts` channel
    must NEVER see typo / bad-arg failures even if the response_url
    delivery happens to fail. Regression guard for the design choice
    that the bot-token fallback is operational-only.
    """
    from scripts import wikimedia_launch

    posted_to_channel: list[tuple[str, str]] = []

    monkeypatch.setenv("DPLA_SLACK_BOT_TOKEN", "fake-token")
    # Simulate response_url being unreachable — would trigger the
    # fallback if `operational=True` were set. User-error path must NOT
    # call post_message (which goes to #tech-alerts).
    monkeypatch.setattr(wikimedia_launch.requests, "post", _raise_connect_timeout)
    monkeypatch.setattr(
        wikimedia_launch,
        "post_message",
        lambda token, text: posted_to_channel.append((token, text)),
    )

    with pytest.raises(SystemExit) as excinfo:
        wikimedia_launch._slack_fail(
            "https://hooks.slack.com/commands/T/N/X",
            "Could not parse --partner: bad token",
        )
    assert excinfo.value.code == 1
    assert posted_to_channel == [], (
        "user-error _slack_fail must NOT post to #tech-alerts, even when "
        f"response_url fails; got: {posted_to_channel!r}"
    )


def test_slack_fail_operational_falls_back_to_channel_when_response_url_fails(
    monkeypatch,
):
    """Operational `_slack_fail` call (operational=True) tries the
    response_url first, then falls back to posting to `#tech-alerts`
    via DPLA_SLACK_BOT_TOKEN when response_url is unreachable.

    Regression for the network-blackout case where the GH runner
    couldn't reach hooks.slack.com (and AWS SSM); user got no Slack
    notification of the launch failure because the only delivery path
    was via the same hostname class that was down.
    """
    from scripts import wikimedia_launch

    posted_to_channel: list[tuple[str, str]] = []

    monkeypatch.setenv("DPLA_SLACK_BOT_TOKEN", "fake-token")
    monkeypatch.setattr(wikimedia_launch.requests, "post", _raise_connect_timeout)
    monkeypatch.setattr(
        wikimedia_launch,
        "post_message",
        lambda token, text: posted_to_channel.append((token, text)),
    )

    with pytest.raises(SystemExit):
        wikimedia_launch._slack_fail(
            "https://hooks.slack.com/commands/T/N/X",
            "⚠️ Failed to update EC2 code: connect timeout",
            operational=True,
        )
    assert len(posted_to_channel) == 1, (
        "operational _slack_fail must fall back to #tech-alerts when "
        f"response_url fails; got: {posted_to_channel!r}"
    )
    token, text = posted_to_channel[0]
    assert token == "fake-token"
    assert "response_url unreachable" in text
    assert "⚠️ Failed to update EC2 code" in text


def test_slack_fail_operational_skips_fallback_when_response_url_succeeds(
    monkeypatch,
):
    """If response_url delivery succeeded, the operational fallback must
    NOT also post to `#tech-alerts` — that would duplicate the
    notification (private to the user AND public). The fallback only
    fires when the primary delivery actually failed."""
    from scripts import wikimedia_launch

    posted_to_channel: list[tuple[str, str]] = []

    class _FakeResponse:
        def raise_for_status(self):
            pass

    monkeypatch.setenv("DPLA_SLACK_BOT_TOKEN", "fake-token")
    monkeypatch.setattr(
        wikimedia_launch.requests, "post", lambda *a, **kw: _FakeResponse()
    )
    monkeypatch.setattr(
        wikimedia_launch,
        "post_message",
        lambda token, text: posted_to_channel.append((token, text)),
    )

    with pytest.raises(SystemExit):
        wikimedia_launch._slack_fail(
            "https://hooks.slack.com/commands/T/N/X",
            "⚠️ Failed to launch tmux session: connect timeout",
            operational=True,
        )
    assert posted_to_channel == [], (
        "fallback must NOT fire when response_url delivery succeeded; "
        f"got duplicate channel post: {posted_to_channel!r}"
    )


def test_slack_fail_operational_silent_when_no_bot_token(monkeypatch):
    """If DPLA_SLACK_BOT_TOKEN is unset AND response_url fails, the
    operational `_slack_fail` should still exit cleanly (logging a
    warning) — and must NOT attempt the fallback post_message call
    with an empty token. A future refactor that lost the empty-token
    guard would crash with a Slack `invalid_auth` error; this test
    locks in the "silent when no token" contract by asserting
    post_message is never invoked."""
    from scripts import wikimedia_launch

    posted_to_channel: list[tuple[str, str]] = []

    monkeypatch.delenv("DPLA_SLACK_BOT_TOKEN", raising=False)
    monkeypatch.setattr(wikimedia_launch.requests, "post", _raise_connect_timeout)
    monkeypatch.setattr(
        wikimedia_launch,
        "post_message",
        lambda token, text: posted_to_channel.append((token, text)),
    )

    with pytest.raises(SystemExit) as excinfo:
        wikimedia_launch._slack_fail(
            "https://hooks.slack.com/commands/T/N/X",
            "⚠️ Failed to update EC2 code: connect timeout",
            operational=True,
        )
    assert excinfo.value.code == 1  # still exits 1, no traceback
    assert posted_to_channel == [], (
        "fallback must NOT attempt post_message with an empty token; "
        f"got: {posted_to_channel!r}"
    )


@pytest.mark.parametrize("bad_value", ["0", "-1", "abc", "1.5"])
def test_invalid_workers_fails_fast(bad_value):
    """--workers must be an integer >= 1. Anything else fails the launch
    with a clear error rather than shelling a bogus value to EC2."""
    exit_info = _run_main(["--partner", "minnesota", "--workers", bad_value])
    assert exit_info is not None, f"expected SystemExit for --workers {bad_value!r}"
    assert exit_info.code == 1


@pytest.mark.parametrize("bad_value", ["-1", "abc", "1.5"])
def test_invalid_workers_budget_fails_fast(bad_value):
    """--workers-budget must be an integer >= 0 (0 disables). Negative or
    non-integer values fail fast."""
    exit_info = _run_main(["--partner", "minnesota", "--workers-budget", bad_value])
    assert exit_info is not None, f"expected SystemExit for budget {bad_value!r}"
    assert exit_info.code == 1


def test_workers_budget_zero_is_accepted(capsys):
    """--workers-budget 0 is the explicit 'disabled' value and must NOT
    trip the validation gate (0 is a valid sentinel, distinct from a
    negative error)."""
    exit_info = _run_main(["--partner", "not-a-real-hub", "--workers-budget", "0"])
    assert exit_info is not None and exit_info.code == 1, (
        "expected SystemExit(1) for the bogus partner after budget validation"
    )
    err = capsys.readouterr().err
    assert "Invalid --workers-budget value" not in err, (
        f"--workers-budget 0 must pass the budget gate, not trip it; got: {err!r}"
    )


def _build(canonical, institutions=(), collection=None, dpla_id=None):
    import scripts.wikimedia_launch as launch_mod

    return launch_mod._build_get_ids_command(
        canonical, institutions, collection, dpla_id, "out.csv"
    )


def test_get_ids_command_nara_hub_uses_get_ids_nara():
    """Hub-level NARA with no institution/collection takes the bespoke
    get-ids-nara catalog walk."""
    assert _build("nara") == "get-ids-nara > out.csv"


def test_get_ids_command_nara_collection_routes_to_get_ids_es():
    """A NARA *collection* target (nara||collection) must go through
    get-ids-es — get-ids-nara has no collection filter and would silently
    ingest the entire hub instead of the requested collection."""
    cmd = _build("nara", collection="General Records of the United States Government")
    assert cmd.startswith("get-ids-es nara")
    assert "get-ids-nara" not in cmd
    assert "--collection 'General Records of the United States Government'" in cmd
    assert "--institution" not in cmd


def test_get_ids_command_hub_wide_collection_omits_institution():
    """Hub-wide collection on a non-NARA hub: --collection with no
    --institution, matched across every eligible institution."""
    cmd = _build("bpl", collection="Maps")
    assert cmd == "get-ids-es bpl --collection Maps > out.csv"


def test_get_ids_command_institution_collection_combines_both():
    cmd = _build("bpl", institutions=("Boston Public Library",), collection="Maps")
    assert cmd == (
        "get-ids-es bpl --institution 'Boston Public Library' --collection Maps > out.csv"
    )


def test_get_ids_command_multiple_institutions_repeat_flag():
    cmd = _build("bpl", institutions=("A", "B"))
    assert cmd == "get-ids-es bpl --institution A --institution B > out.csv"


def test_get_ids_command_single_id_takes_precedence():
    """--single-id re-stages via get-ids-es regardless of hub (incl. NARA)."""
    cmd = _build("nara", dpla_id="abc123")
    assert cmd == "get-ids-es nara --single-id abc123 > out.csv"


def _label(canonical, institutions=(), collection=None):
    import scripts.wikimedia_launch as launch_mod

    return launch_mod._target_label(canonical, institutions, collection)


def test_target_label_hub_wide_collection_does_not_crash():
    """A hub-wide collection target (institutions=()) must render without an
    IndexError on the Slack message path — it has no institution to index."""
    assert _label("nara", collection="General Records") == "`nara||General Records`"


def test_target_label_institution_collection():
    assert _label("bpl", ("Boston Public Library",), "Maps") == (
        "`bpl|Boston Public Library|Maps`"
    )


def test_target_label_single_and_multi_institution():
    assert _label("bpl", ("A",)) == "`bpl|A`"
    assert _label("bpl", ("A", "B", "C")) == "`bpl|A (+2 more)`"


def test_target_label_hub_only():
    assert _label("bpl") == "`bpl`"


def test_count_only_requires_maintain():
    exit_info = _run_main(["--partner", "digitalnc", "--count-only", "true"])
    assert exit_info is not None
    assert exit_info.code == 1


def _maintain_steps(
    canonical,
    institutions,
    count_only,
    *,
    collection=None,
    dpla_id=None,
    worker_opts="",
):
    """Run _build_maintain_pipeline_steps with Wikidata resolution stubbed."""
    import scripts.wikimedia_launch as launch_mod

    with (
        patch.object(launch_mod, "wikidata_qid_for_target", return_value="Q123"),
        patch.object(
            launch_mod,
            "resolve_commons_category",
            return_value="Category:Media contributed by X",
        ),
    ):
        return launch_mod._build_maintain_pipeline_steps(
            canonical,
            institutions,
            collection,
            dpla_id,
            "/srv/base",
            "out.csv",
            count_only,
            worker_opts=worker_opts,
        )


def test_maintain_real_run_stages_sidecars_then_syncs_from_s3():
    steps = _maintain_steps("digitalnc", (), count_only=False)
    # One ES scan stages sdc.json sidecars, then the category walk reads them.
    assert steps[0] == "cd /srv/base"
    assert steps[1] == "get-ids-es digitalnc --maintain > out.csv"
    assert steps[2] == (
        "sdc-sync --cat 'Category:Media contributed by X' --maintain"
        " --from-s3 digitalnc"
    )
    assert "--count-only" not in steps[2]


def test_maintain_per_institution_get_ids_repeats_institution_flags():
    steps = _maintain_steps(
        "georgia", ("Athens-Clarke County Library", "Atlanta History Center"), False
    )
    assert steps[1] == (
        "get-ids-es georgia --institution 'Athens-Clarke County Library'"
        " --institution 'Atlanta History Center' --maintain > out.csv"
    )
    # One --cat sync per institution category, all reading from S3.
    syncs = [s for s in steps if s.startswith("sdc-sync")]
    assert len(syncs) == 2
    assert all("--from-s3 georgia" in s for s in syncs)


def test_maintain_count_only_skips_staging_and_from_s3():
    steps = _maintain_steps("digitalnc", (), count_only=True)
    assert steps == [
        "cd /srv/base",
        "sdc-sync --cat 'Category:Media contributed by X' --maintain --count-only",
    ]
    # Pre-flight sizing never stages sidecars and never reads from S3.
    assert not any("get-ids-es" in s for s in steps)
    assert not any("--from-s3" in s for s in steps)


def test_maintain_write_run_passes_worker_opts_to_cat_sync():
    steps = _maintain_steps(
        "digitalnc", (), False, worker_opts=" --workers 6 --workers-budget 24"
    )
    sync = next(s for s in steps if s.startswith("sdc-sync"))
    # Parallel maintain runs the pool under the box-wide slot budget, same flags
    # as partner mode, appended after --from-s3.
    assert sync.endswith("--from-s3 digitalnc --workers 6 --workers-budget 24")


def test_maintain_count_only_ignores_worker_opts():
    steps = _maintain_steps(
        "digitalnc", (), True, worker_opts=" --workers 6 --workers-budget 24"
    )
    sync = next(s for s in steps if s.startswith("sdc-sync"))
    # count-only is serial read-only sizing — never parallelized.
    assert "--workers" not in sync
    assert sync.endswith("--maintain --count-only")


def test_maintain_rejects_collection_and_single_id_targets():
    coll = _maintain_steps("georgia", (), False, collection="Maps")
    assert coll[-1].endswith("exit 1")
    assert "does not support" in coll[-1]
    single = _maintain_steps("georgia", (), False, dpla_id="abc123")
    assert single[-1].endswith("exit 1")


def test_maintain_mutually_exclusive_with_sdc_only():
    exit_info = _run_main(
        ["--partner", "georgia", "--maintain", "true", "--sdc-only", "true"]
    )
    assert exit_info is not None
    assert exit_info.code == 1


def test_maintain_mutually_exclusive_with_refresh_only():
    exit_info = _run_main(
        ["--partner", "georgia", "--maintain", "true", "--refresh-only", "true"]
    )
    assert exit_info is not None
    assert exit_info.code == 1


def test_maintain_bypasses_upload_eligibility_gate(monkeypatch, capsys):
    """`--maintain true` for an upload-INELIGIBLE hub must parse and get PAST
    the eligibility gate — maintain operates only on files already on Commons,
    so ineligibility is exactly when it applies. Stub the EC2 path so the exit
    comes from a deterministic post-parse point, then assert it is neither the
    mutual-exclusion error nor a 'not upload-eligible' skip."""
    monkeypatch.setattr(
        "scripts.wikimedia_launch.boto3.client", lambda *a, **kw: object()
    )
    monkeypatch.setattr(
        "scripts.wikimedia_launch.ssm_run",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("ssm stubbed")),
    )
    # "digitalnc" (North Carolina Digital Heritage Center) is upload=false in
    # institutions_v2.json — the canonical maintain target.
    exit_info = _run_main(["--partner", "digitalnc", "--maintain", "true"])
    assert exit_info is not None
    captured = capsys.readouterr()
    assert "mutually exclusive" not in captured.err
    assert "not upload-eligible" not in captured.err
