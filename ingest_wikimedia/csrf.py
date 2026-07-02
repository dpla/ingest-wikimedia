"""Shared CSRF-session-recovery primitives for every Wikimedia write path.

Pywikibot's :class:`TokenWallet` raises
``KeyError("Invalid token 'csrf' for user '<bot>' on commons:commons
wiki.")`` when the cached session is invalidated by the server (long
idle, forced logout, backend reset). Every subsequent write attempt
raises the same ``KeyError`` — retrying the same call cannot recover;
the session itself has to be dropped and re-established.

PR #350 first fixed this for :mod:`tools.uploader`. The Toledo Lucas
County SDC-sync run of 2026-06-25 then surfaced 68,411 identical CSRF
errors bucketed as "SDC sync failed; skipping ordinal" over ~5.5 days
— :func:`_submit_sdc_write`, :func:`FilePage.touch`,
:meth:`ItemPage.editEntity` and :meth:`FilePage.save` all had the same
weakness. This module extracts the detector, recovery function, and
run-scoped cap into one place so every write path shares the same
guardrails.

## The invariants callers must maintain

* Recovery is **per-process**: the counter lives on this module and
  isolates naturally to one process. Under
  ``tools.sdc_sync._run_partner_mode_parallel`` (spawn multiprocessing)
  each worker imports its own copy of :mod:`ingest_wikimedia.csrf`, so
  the effective cap is ``N × MAX_CSRF_RECOVERIES`` across N workers —
  that's the intended design (each worker has its own pywikibot
  session and can independently need refreshing) but callers should
  read the cap that way rather than as "3 total per run". Within one
  process, every writer shares the counter: never introduce an
  instance-scoped duplicate.
* The cap :data:`MAX_CSRF_RECOVERIES` is a hard guardrail against
  unbounded loops on a persistently invalid session. Exceeding it
  raises :class:`CsrfRecoveryFailed`, which callers should let
  propagate past their generic per-item ``except Exception`` handlers
  so the whole run aborts (mirrors the uploader's abort contract).
* Only WRITE paths need this — :func:`is_csrf_token_error` only fires
  on the specific ``KeyError`` from ``site.tokens['csrf']``. Read-only
  API calls (``action=query``, ``wbgetentities``) never fetch the
  csrf token, so they can't trip this error.
"""

from __future__ import annotations

import logging
from typing import Callable, TypeVar


class CsrfRecoveryFailed(RuntimeError):
    """Raised when the pywikibot session's CSRF token cannot be recovered.

    Callers should route this AROUND their per-item generic
    ``except Exception`` catches so a session-level fatal aborts the
    run instead of looping the same error against every remaining item.
    """


MAX_CSRF_RECOVERIES = 3

CSRF_TOKEN_ERROR_MARKER = "Invalid token 'csrf'"


def is_csrf_token_error(ex: BaseException) -> bool:
    """True iff ``ex`` (or any exception in its ``__cause__`` chain)
    signals an invalidated CSRF token. Recognises two shapes:

    1. Pywikibot's ``TokenWallet.__getitem__`` raises ``KeyError`` with
       the :data:`CSRF_TOKEN_ERROR_MARKER` message — the wallet
       couldn't produce a valid csrf token BEFORE we ever hit the wire
       (session invalidated / wallet miss). This is the Toledo
       2026-06-25 fingerprint.
    2. Pywikibot's ``simple_request(...).submit()`` propagates an
       ``APIError`` with ``code='badtoken'`` when the server rejected
       our submitted token AND pywikibot's own internal relogin
       didn't recover it — the wire-level twin of #1.

    Walks ``__cause__`` because callers commonly wrap the underlying
    pywikibot exception in a domain-specific ``RuntimeError`` (see
    :func:`tools.sdc_sync._submit_sdc_write`), which would otherwise
    hide the CSRF signal from :func:`with_csrf_recovery`.
    """
    seen: set[int] = set()
    cur: BaseException | None = ex
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, KeyError) and CSRF_TOKEN_ERROR_MARKER in str(cur):
            return True
        # Duck-typed on ``.code`` to avoid pulling pywikibot into this
        # module's imports (test paths import this without pywikibot
        # installed). ``APIError`` is the only exception in the write
        # path that exposes ``.code``, so a false positive here
        # requires a foreign exception class deliberately shadowing
        # both ``.code`` and the string value ``'badtoken'`` — not a
        # realistic collision.
        if getattr(cur, "code", None) == "badtoken":
            return True
        cur = getattr(cur, "__cause__", None)
    return False


def recover_commons_session(site) -> None:
    """Drop the current pywikibot session, re-authenticate, and clear
    the cached ``TokenWallet`` so the next ``site.tokens['csrf']``
    fetch talks to a fresh session.

    ``site.logout()`` is required first: ``site.login()`` on an
    already-"logged-in" site is a no-op in pywikibot's default flow,
    so clearing tokens alone would just re-fetch against the same
    invalidated session.
    """
    logging.info("Recovering Commons session: logout + login + token-wallet clear")
    site.logout()
    site.login()
    site.tokens.clear()


# Run-scoped counter — module state so every write path in the process
# shares the same cap. Reset at process start (import initializes to 0);
# not intended to be reset mid-run.
_session_recoveries_used = 0


def session_recoveries_used() -> int:
    """Return the number of CSRF recoveries the current process has
    consumed so far. Used by callers that want to log recovery
    progress ("recovery N/MAX") before invoking one."""
    return _session_recoveries_used


def bump_session_recovery() -> int:
    """Record that a CSRF recovery has been attempted. Returns the new
    count. Callers should call this AFTER
    :func:`recover_commons_session` returns successfully so a failed
    recovery attempt doesn't burn a slot."""
    global _session_recoveries_used
    _session_recoveries_used += 1
    return _session_recoveries_used


T = TypeVar("T")


def reset_session_recoveries() -> None:
    """Zero the recovery counter — used to signal that a write against
    the current session succeeded, so the cap should be interpreted as
    "consecutive failed recoveries" rather than a lifetime budget.

    A 5.5-day partner-mode run can legitimately have the session go
    stale more than :data:`MAX_CSRF_RECOVERIES` times over its lifetime
    (Wikimedia session policy + long idle windows). Under a lifetime
    cap, run N+1 stale-then-refresh events would abort the whole run
    even though every recovery worked. Consecutive-cap semantics only
    abort when the session is genuinely un-refreshable — which is what
    :class:`CsrfRecoveryFailed` is meant to signal."""
    global _session_recoveries_used
    _session_recoveries_used = 0


def with_csrf_recovery(site, action_label: str, thunk: Callable[[], T]) -> T:
    """Call ``thunk()``; on CSRF error, refresh the session and retry.
    Repeat until either ``thunk()`` succeeds or
    :data:`MAX_CSRF_RECOVERIES` consecutive refreshes still can't
    unstick the token — at which point :class:`CsrfRecoveryFailed` is
    raised. Non-CSRF exceptions propagate immediately.

    ``action_label`` is a short human-readable tag used only in the
    warning log line (e.g. ``"wbeditentity M12345"``, ``"touch File:X"``).

    **Cap semantics: consecutive, not lifetime.** After every
    successful ``thunk()`` return the counter is reset to zero, so a
    long-running process that legitimately experiences a stale session
    N times over its lifetime — each recovered — never trips the cap.
    The cap only fires when :data:`MAX_CSRF_RECOVERIES` refreshes in a
    row fail to produce a working session, which is the genuine
    "session is un-refreshable, abort the run" signal.

    Raises :class:`CsrfRecoveryFailed` when the cap is exhausted or
    when :func:`recover_commons_session` itself throws — callers
    should let this propagate past their per-item catches so a
    persistently invalid session aborts the run rather than looping
    against every remaining item.

    This helper is for write sites that don't already own a retry loop
    (e.g. ``touch()``, ``editEntity()``, ``save()``). Sites that own
    their own attempt-counted loop (:mod:`tools.uploader`) integrate
    the CSRF branch directly and MUST also call
    :func:`reset_session_recoveries` on successful writes to keep the
    consecutive-cap semantics consistent across the shared counter.
    """
    while True:
        try:
            result = thunk()
        except Exception as ex:
            if not is_csrf_token_error(ex):
                raise
            if session_recoveries_used() >= MAX_CSRF_RECOVERIES:
                raise CsrfRecoveryFailed(
                    f"Commons session invalidated on {action_label}: CSRF"
                    f" token still invalid after {session_recoveries_used()}"
                    f" consecutive recovery attempts; aborting run."
                ) from ex
            logging.warning(
                "CSRF token invalidated on %s; refreshing Commons session"
                " (recovery %d/%d)",
                action_label,
                session_recoveries_used() + 1,
                MAX_CSRF_RECOVERIES,
            )
            try:
                recover_commons_session(site)
            except Exception as recover_ex:
                raise CsrfRecoveryFailed(
                    f"Commons session recovery threw on {action_label}"
                    f" ({recover_ex!r}); aborting rather than looping"
                    f" unrecoverable auth errors."
                ) from recover_ex
            bump_session_recovery()
            # Loop and retry the thunk against the recovered session.
            continue
        # Successful write against the current session — reset the
        # consecutive-recovery counter. See docstring above.
        reset_session_recoveries()
        return result
