"""Tests for the shared CSRF-recovery primitives in
``ingest_wikimedia.csrf``.

Motivated by the Toledo Lucas 2026-06-25 SDC-sync run: 68,411 identical
``KeyError("Invalid token 'csrf' ...")`` failures bucketed as
"SDC sync failed; skipping ordinal" over ~5.5 days. PR #350 had already
solved the same class of bug for :mod:`tools.uploader`; this suite
locks the shared abstraction that every write path now consumes.
"""

from unittest.mock import MagicMock

import pytest

from ingest_wikimedia import csrf


def _reset_recovery_counter():
    """Zero the module-level session counter so a test doesn't inherit
    another test's recoveries. Not a public API — deliberately reaches
    at the private name — because a public reset would invite misuse
    at runtime."""
    csrf._session_recoveries_used = 0


@pytest.fixture(autouse=True)
def _fresh_counter():
    """Every test starts with the counter at 0."""
    _reset_recovery_counter()
    yield
    _reset_recovery_counter()


def test_is_csrf_token_error_matches_the_wallet_keyerror():
    """The pywikibot ``TokenWallet.__getitem__`` failure produces a
    ``KeyError`` whose message carries the exact marker below. That
    marker + the ``KeyError`` type together are the narrow signal for
    "session invalidated" — the whole point of the detector is that
    it doesn't fire on ANY ``KeyError`` (a dict miss elsewhere in the
    code must never trigger session recovery)."""
    real = KeyError("Invalid token 'csrf' for user 'DPLA bot' on commons:commons wiki.")
    assert csrf.is_csrf_token_error(real)


def test_is_csrf_token_error_rejects_unrelated_keyerror():
    assert not csrf.is_csrf_token_error(KeyError("some_dict_key"))


def test_is_csrf_token_error_matches_apierror_badtoken():
    """The wire-level twin of the wallet KeyError: pywikibot's
    ``APIError`` with ``code='badtoken'`` reaches us when the server
    rejected our submitted token AND pywikibot's own internal relogin
    couldn't recover it. Duck-typed on ``.code`` so the detector
    doesn't need to import pywikibot."""

    class FakeAPIError(Exception):
        def __init__(self, code, info=""):
            super().__init__(f"{code}: {info}")
            self.code = code
            self.info = info

    assert csrf.is_csrf_token_error(FakeAPIError("badtoken", "Invalid CSRF token."))
    # Not a badtoken error — any other pywikibot APIError code follows
    # the caller's existing per-item skip path.
    assert not csrf.is_csrf_token_error(FakeAPIError("ratelimited", "Slow down."))
    assert not csrf.is_csrf_token_error(FakeAPIError("no-such-entity", "Missing."))


def test_is_csrf_token_error_walks_cause_chain():
    """``_submit_sdc_write`` wraps a pywikibot ``APIError(badtoken)``
    in a ``RuntimeError(...) from e`` — without walking ``__cause__``
    the detector would miss it and the CSRF write would fall through
    to the caller's generic skip path, defeating the recovery
    contract."""

    class FakeAPIError(Exception):
        def __init__(self, code):
            super().__init__(code)
            self.code = code

    try:
        try:
            raise FakeAPIError("badtoken")
        except Exception as inner:
            raise RuntimeError("wbeditentity failed for M123: badtoken") from inner
    except RuntimeError as outer:
        assert csrf.is_csrf_token_error(outer)


def test_is_csrf_token_error_walks_cause_chain_for_wallet_keyerror():
    """Same chain-walking contract for the wallet-side ``KeyError``:
    any caller that wraps the underlying token failure in another
    exception must still route through recovery."""

    try:
        try:
            raise KeyError(
                "Invalid token 'csrf' for user 'DPLA bot' on commons:commons wiki."
            )
        except Exception as inner:
            raise RuntimeError("outer wrapper") from inner
    except RuntimeError as outer:
        assert csrf.is_csrf_token_error(outer)


def test_is_csrf_token_error_terminates_on_circular_cause_chain():
    """Cycle-guard: a maliciously (or accidentally) circular
    ``__cause__`` chain must not spin forever. Realistically
    ``__cause__`` chains built via ``raise ... from ex`` never cycle,
    but defense-in-depth keeps the detector total."""
    a = RuntimeError("a")
    b = RuntimeError("b")
    a.__cause__ = b
    b.__cause__ = a
    # Doesn't hang; returns False because neither exception matches.
    assert not csrf.is_csrf_token_error(a)


def test_is_csrf_token_error_rejects_non_keyerror_with_matching_message():
    """A RuntimeError whose message happens to contain the marker must
    NOT trigger recovery — pywikibot only ever raises this as a
    ``KeyError``, and a random exception happening to mention "Invalid
    token 'csrf'" (e.g. an APIError echoing the server's info string)
    should follow the normal per-item handler."""
    assert not csrf.is_csrf_token_error(
        RuntimeError("upstream said: Invalid token 'csrf' apparently")
    )


def test_recover_commons_session_calls_logout_login_and_clears_tokens():
    """The three-step recovery is load-bearing:
    ``site.login()`` on an already-logged-in site is a no-op in
    pywikibot's default flow, so ``logout()`` must precede it; and
    the ``TokenWallet`` caches tokens per-session, so ``clear()`` is
    needed to force the next ``site.tokens['csrf']`` fetch."""
    site = MagicMock()
    csrf.recover_commons_session(site)
    site.logout.assert_called_once()
    site.login.assert_called_once()
    site.tokens.clear.assert_called_once()


def test_session_counter_is_run_scoped_across_calls():
    """A stale session affects every writer in the process, so the
    counter must be module-level (shared) — not instance-scoped
    (per-writer). Verify successive ``bump_session_recovery`` calls
    accumulate."""
    assert csrf.session_recoveries_used() == 0
    csrf.bump_session_recovery()
    assert csrf.session_recoveries_used() == 1
    csrf.bump_session_recovery()
    assert csrf.session_recoveries_used() == 2


def test_with_csrf_recovery_passes_through_on_success():
    """The happy path: the thunk returns cleanly and its value comes
    straight back — no session touched, no recovery consumed."""
    site = MagicMock()
    out = csrf.with_csrf_recovery(site, "test-action", lambda: "ok")
    assert out == "ok"
    site.logout.assert_not_called()
    assert csrf.session_recoveries_used() == 0


def test_with_csrf_recovery_reraises_non_csrf_exceptions_immediately():
    """A non-CSRF exception must NOT trigger session recovery — the
    whole point of the narrow detector is that unrelated failures
    (network, APIError, etc.) follow the caller's existing
    per-item handler."""
    site = MagicMock()
    with pytest.raises(RuntimeError, match="other error"):
        csrf.with_csrf_recovery(
            site,
            "test-action",
            lambda: (_ for _ in ()).throw(RuntimeError("other error")),
        )
    site.logout.assert_not_called()
    assert csrf.session_recoveries_used() == 0


def test_with_csrf_recovery_refreshes_and_retries_on_csrf_error():
    """On the FIRST CSRF error the helper drops the session, retries
    the thunk against the fresh session, and returns the second
    attempt's value. This is the exact recovery pattern the Toledo
    run needed: one bad token → refresh → succeed on the next call.
    """
    site = MagicMock()
    attempts = {"n": 0}

    def _thunk():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise KeyError(
                "Invalid token 'csrf' for user 'DPLA bot' on commons:commons wiki."
            )
        return "recovered"

    out = csrf.with_csrf_recovery(site, "test-action", _thunk)
    assert out == "recovered"
    assert attempts["n"] == 2
    site.logout.assert_called_once()
    site.login.assert_called_once()
    site.tokens.clear.assert_called_once()
    # Counter resets to 0 after the successful retry — consecutive-cap
    # semantics. The recovery HAPPENED (site.logout/login/clear all
    # asserted above), but a successful thunk zeroes the streak.
    assert csrf.session_recoveries_used() == 0


def test_with_csrf_recovery_raises_when_cap_exhausted():
    """Once the run-scoped cap (:data:`MAX_CSRF_RECOVERIES`) is hit,
    the next CSRF error raises :class:`CsrfRecoveryFailed` instead
    of looping — the guardrail against unbounded recovery on a
    persistently invalid session. The raised exception must NOT be a
    plain :class:`KeyError` (which would follow the per-item skip
    path) so a session-level fatal actually aborts the run."""
    site = MagicMock()
    # Prime the counter to the cap.
    for _ in range(csrf.MAX_CSRF_RECOVERIES):
        csrf.bump_session_recovery()

    def _always_csrf():
        raise KeyError(
            "Invalid token 'csrf' for user 'DPLA bot' on commons:commons wiki."
        )

    with pytest.raises(csrf.CsrfRecoveryFailed):
        csrf.with_csrf_recovery(site, "test-action", _always_csrf)
    # No session touch — cap was already exhausted before we tried.
    site.logout.assert_not_called()


def test_with_csrf_recovery_raises_if_recovery_itself_fails():
    """If :func:`recover_commons_session` throws (e.g. login itself
    fails), the helper wraps that as :class:`CsrfRecoveryFailed`
    rather than letting the raw ``ConnectionError`` bubble past the
    per-item handler as a routine failure. Session-level fatals need
    the ``CsrfRecoveryFailed`` type to route around per-item
    ``except Exception`` catches."""
    site = MagicMock()
    site.logout.side_effect = ConnectionError("network down")

    def _thunk():
        raise KeyError(
            "Invalid token 'csrf' for user 'DPLA bot' on commons:commons wiki."
        )

    with pytest.raises(csrf.CsrfRecoveryFailed):
        csrf.with_csrf_recovery(site, "test-action", _thunk)
    # A failed recovery must NOT bump the counter — otherwise a run
    # could burn its whole recovery budget on non-recovery-attempt
    # failures and hit the cap before a real refresh has been tried.
    assert csrf.session_recoveries_used() == 0


def test_with_csrf_recovery_recovers_multiple_times_up_to_cap():
    """Successive CSRF errors, each recoverable on retry, walk the
    counter up to (but not past) the cap. Only the (cap+1)th error
    raises. Because the final ``thunk()`` DOES succeed, the counter
    resets to 0 — consecutive-cap semantics reward the eventual
    recovery."""
    site = MagicMock()
    seen = {"n": 0}

    def _thunk():
        seen["n"] += 1
        # Fail csrf-style on the first MAX_CSRF_RECOVERIES calls,
        # succeed on the (MAX+1)th.
        if seen["n"] <= csrf.MAX_CSRF_RECOVERIES:
            raise KeyError(
                "Invalid token 'csrf' for user 'DPLA bot' on commons:commons wiki."
            )
        return "eventually-ok"

    out = csrf.with_csrf_recovery(site, "test-action", _thunk)
    assert out == "eventually-ok"
    # Counter reset after the eventual success — even though we walked
    # right to the cap, the successful thunk cleared the streak.
    assert csrf.session_recoveries_used() == 0
    # ...but recovery WAS invoked ``MAX_CSRF_RECOVERIES`` times to get
    # us to the successful attempt.
    assert site.logout.call_count == csrf.MAX_CSRF_RECOVERIES


def test_with_csrf_recovery_resets_counter_on_successful_thunk():
    """Consecutive-cap semantics, not lifetime-cap: a successful
    ``thunk()`` return zeros the counter, so a long-running run that
    legitimately hits N stale-then-refresh events across its lifetime
    doesn't trip the cap. Only :data:`MAX_CSRF_RECOVERIES`
    *consecutive* failed recoveries abort the run."""
    site = MagicMock()
    call = {"n": 0}

    def _flaky_thunk():
        call["n"] += 1
        if call["n"] == 1:
            raise KeyError(
                "Invalid token 'csrf' for user 'DPLA bot' on commons:commons wiki."
            )
        return f"ok-{call['n']}"

    # First cycle: 1 recovery, then success. Counter reset.
    assert csrf.with_csrf_recovery(site, "test-action", _flaky_thunk) == "ok-2"
    assert csrf.session_recoveries_used() == 0, (
        "counter must reset after successful thunk — otherwise long-running "
        "processes exhaust the lifetime budget on legitimate stale sessions"
    )

    # A subsequent independent failure starts fresh at 1, not 2.
    call["n"] = 0
    assert csrf.with_csrf_recovery(site, "test-action", _flaky_thunk) == "ok-2"
    assert csrf.session_recoveries_used() == 0


def test_with_csrf_recovery_counter_persists_across_consecutive_failures():
    """The reset happens only on SUCCESS. A run of consecutive CSRF
    failures (no intervening success) walks the counter up to the cap,
    exactly as before — the persistently-invalid-session guard still
    works."""
    site = MagicMock()

    def _always_csrf():
        raise KeyError(
            "Invalid token 'csrf' for user 'DPLA bot' on commons:commons wiki."
        )

    with pytest.raises(csrf.CsrfRecoveryFailed):
        csrf.with_csrf_recovery(site, "test-action", _always_csrf)
    # All MAX_CSRF_RECOVERIES slots consumed by the consecutive failures.
    assert csrf.session_recoveries_used() == csrf.MAX_CSRF_RECOVERIES


def test_reset_session_recoveries_zeros_the_counter():
    """The explicit reset function exists for callers that own their
    own retry loop (the uploader) — they need to signal a successful
    write to keep the consecutive-cap semantics consistent across the
    shared counter."""
    csrf.bump_session_recovery()
    csrf.bump_session_recovery()
    assert csrf.session_recoveries_used() == 2
    csrf.reset_session_recoveries()
    assert csrf.session_recoveries_used() == 0


def test_csrf_recovery_failed_is_a_runtime_error_subclass():
    """Callers `except Exception` would swallow the abort signal
    otherwise. The subclass identity + the explicit
    ``except CsrfRecoveryFailed: raise`` re-raise pattern in every
    per-item handler together enforce the abort contract."""
    assert issubclass(csrf.CsrfRecoveryFailed, RuntimeError)


def test_max_csrf_recoveries_is_three():
    """Locked at 3 by contract with the uploader (PR #350). Bumping
    this is a design decision, not a mechanical tweak — a stuck
    session usually can't be un-stuck by more attempts."""
    assert csrf.MAX_CSRF_RECOVERIES == 3
