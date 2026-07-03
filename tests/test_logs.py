"""Tests for ``ingest_wikimedia.logs``.

Focused on the ``sys.excepthook`` install: without it, uncaught
exceptions in tools running under tmux print their tracebacks to the
terminal buffer and vanish when the session ends — leaving the
per-tool log file's final line as whatever logged just before the
crash. These tests pin the recovery behavior in place.
"""

import logging
import sys

import pytest

from ingest_wikimedia import logs as logs_mod


@pytest.fixture
def isolated_logging():
    """Snapshot ``sys.excepthook`` and root-logger state around a test.

    Every test in this file mutates one or both. Duplicating the
    save/restore boilerplate in each test invited exactly the kind of
    incomplete cleanup CR flagged (missing ``root.level`` restore,
    unclosed ``FileHandler``) — the fixture centralizes it so a new
    test can't forget a step.

    Cleanup:
      * ``sys.excepthook`` restored.
      * Any handlers attached during the test are removed AND closed
        (``setup_logging`` installs a ``FileHandler`` that opens an
        fd — closing it explicitly avoids leaking it across the test
        session).
      * Root logger's level restored (``logging.basicConfig`` inside
        ``setup_logging`` mutates it).

    Tests that need a capturing handler still install their own; the
    fixture only guarantees teardown, not setup.
    """
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    saved_hook = sys.excepthook
    # Pre-remove existing handlers so ``logging.basicConfig`` inside
    # ``setup_logging`` actually installs its new handlers (basicConfig
    # is a no-op if any handlers are already attached).
    for h in saved_handlers:
        root.removeHandler(h)
    try:
        yield
    finally:
        sys.excepthook = saved_hook
        for h in list(root.handlers):
            root.removeHandler(h)
            h.close()
        for h in saved_handlers:
            root.addHandler(h)
        root.setLevel(saved_level)


def test_setup_logging_installs_uncaught_exception_hook(
    tmp_path, monkeypatch, isolated_logging
):
    """``setup_logging`` must replace ``sys.excepthook`` with a wrapper
    that routes tracebacks through the logging system."""
    monkeypatch.setattr(logs_mod, "LOGS_DIR_BASE", str(tmp_path))
    original_hook = sys.excepthook
    logs_mod.setup_logging("test-partner", "test-event")
    assert sys.excepthook is not original_hook, (
        "setup_logging must replace sys.excepthook so tracebacks reach "
        "the file logger instead of vanishing with the tmux session"
    )


def test_installed_excepthook_routes_traceback_to_root_logger(isolated_logging):
    """End-to-end: the installed hook must produce a log record whose
    ``exc_info`` carries the traceback triple, so any handler attached
    to root (including the FileHandler ``setup_logging`` installs) gets
    the full stack. Weaker mock-based tests would lock in a specific
    implementation of that routing; asserting on the emitted
    LogRecord's ``exc_info`` field pins the behavior instead."""
    captured: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record):
            captured.append(record)

    root = logging.getLogger()
    root.addHandler(_Capture(level=logging.CRITICAL))
    root.setLevel(logging.CRITICAL)

    logs_mod._install_logging_excepthook()
    try:
        raise RuntimeError("crash-payload")
    except RuntimeError:
        sys.excepthook(*sys.exc_info())

    assert len(captured) == 1
    rec = captured[0]
    assert rec.levelno == logging.CRITICAL
    assert rec.exc_info is not None, (
        "the LogRecord must carry exc_info so FileHandler renders the "
        "traceback — without it, only the message string is written"
    )
    assert rec.exc_info[0] is RuntimeError
    assert "crash-payload" in str(rec.exc_info[1])


def test_installed_excepthook_is_idempotent(isolated_logging):
    """A repeat install must be a no-op. Without the guard, a caller
    that invoked ``setup_logging`` twice (e.g. a future partner-mode
    worker re-initializing per file) would wrap the hook around
    itself, so a single uncaught exception would log at CRITICAL N
    times where N is the install count."""
    logs_mod._install_logging_excepthook()
    first = sys.excepthook
    logs_mod._install_logging_excepthook()
    assert sys.excepthook is first, (
        "second _install_logging_excepthook call must be a no-op; "
        "wrapping the hook again would produce duplicate CRITICAL logs "
        "per uncaught exception"
    )


def test_installed_excepthook_delegates_to_previous_hook_in_chain(isolated_logging):
    """The install captures ``sys.excepthook`` BEFORE overwriting it,
    so a caller that had already installed an outer hook (pywikibot,
    click, pytest, a debugger) stays in the chain. Delegating to
    ``sys.__excepthook__`` (the untouched default) instead would
    silently discard any such outer wrapper."""
    prev_called: list[tuple] = []

    def _prev(exc_type, exc_value, exc_tb):
        prev_called.append((exc_type, exc_value, exc_tb))

    sys.excepthook = _prev
    logs_mod._install_logging_excepthook()
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        sys.excepthook(*sys.exc_info())

    assert len(prev_called) == 1, (
        "installed hook must delegate to the previously-installed hook "
        "— otherwise an outer wrapper is silently discarded"
    )
    assert prev_called[0][0] is RuntimeError


def test_installed_excepthook_skips_logging_for_keyboard_interrupt(isolated_logging):
    """Ctrl-C is operator-driven, not a crash. Logging it at CRITICAL
    would fill the log with false alarms on every deliberate abort."""
    captured: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record):
            captured.append(record)

    logging.getLogger().addHandler(_Capture(level=logging.DEBUG))
    logs_mod._install_logging_excepthook()
    try:
        raise KeyboardInterrupt()
    except KeyboardInterrupt:
        sys.excepthook(*sys.exc_info())

    assert captured == [], (
        "Ctrl-C must not log at CRITICAL — the file would fill with "
        "false alarms on every operator abort"
    )


def test_installed_excepthook_skips_logging_for_system_exit(isolated_logging):
    """``sys.exit(0)`` (or any ``SystemExit``) is a clean-exit signal,
    not a crash. Logging every clean exit at CRITICAL would produce
    spurious tracebacks in the log for tools that use ``sys.exit()``
    as a normal control-flow primitive."""
    captured: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record):
            captured.append(record)

    logging.getLogger().addHandler(_Capture(level=logging.DEBUG))
    logs_mod._install_logging_excepthook()
    try:
        raise SystemExit(0)
    except SystemExit:
        sys.excepthook(*sys.exc_info())

    assert captured == [], (
        "SystemExit must not log at CRITICAL — every sys.exit() would "
        "otherwise produce a spurious traceback in the log"
    )
