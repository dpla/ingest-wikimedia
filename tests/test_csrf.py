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
    assert csrf.session_recoveries_used() == 1


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
    raises."""
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
    assert csrf.session_recoveries_used() == csrf.MAX_CSRF_RECOVERIES
    assert site.logout.call_count == csrf.MAX_CSRF_RECOVERIES


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
