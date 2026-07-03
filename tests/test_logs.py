"""Tests for ``ingest_wikimedia.logs``.

Focused on the ``sys.excepthook`` install: without it, uncaught
exceptions in tools running under tmux print their tracebacks to the
terminal buffer and vanish when the session ends — leaving the
per-tool log file's final line as whatever logged just before the
crash. These tests pin the recovery behavior in place.
"""

import logging
import sys

from ingest_wikimedia import logs as logs_mod


def test_setup_logging_installs_uncaught_exception_hook(tmp_path, monkeypatch):
    """``setup_logging`` must replace ``sys.excepthook`` with a wrapper
    that routes tracebacks through the logging system."""
    monkeypatch.setattr(logs_mod, "LOGS_DIR_BASE", str(tmp_path))
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    for h in saved_handlers:
        root.removeHandler(h)
    original_hook = sys.excepthook
    try:
        logs_mod.setup_logging("test-partner", "test-event")
        assert sys.excepthook is not original_hook, (
            "setup_logging must replace sys.excepthook so tracebacks "
            "reach the file logger instead of vanishing with the tmux "
            "session"
        )
    finally:
        sys.excepthook = original_hook
        # setup_logging installs a FileHandler that opens a file
        # descriptor — close it explicitly before dropping the handler
        # so the fd doesn't outlive the test.
        for h in list(root.handlers):
            root.removeHandler(h)
            h.close()
        for h in saved_handlers:
            root.addHandler(h)
        root.setLevel(saved_level)


def test_installed_excepthook_routes_traceback_to_root_logger():
    """End-to-end: the installed hook must produce a log record whose
    ``exc_info`` carries the traceback triple, so any handler attached
    to root (including the FileHandler ``setup_logging`` installs) gets
    the full stack. Weaker mock-based tests would lock in a specific
    implementation of that routing; asserting on the emitted
    LogRecord's ``exc_info`` field pins the behavior instead."""
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    for h in saved_handlers:
        root.removeHandler(h)
    original_hook = sys.excepthook
    captured: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record):
            captured.append(record)

    root.addHandler(_Capture(level=logging.CRITICAL))
    root.setLevel(logging.CRITICAL)
    try:
        logs_mod._install_logging_excepthook()
        try:
            raise RuntimeError("crash-payload")
        except RuntimeError:
            sys.excepthook(*sys.exc_info())
    finally:
        sys.excepthook = original_hook
        for h in list(root.handlers):
            root.removeHandler(h)
        for h in saved_handlers:
            root.addHandler(h)
        root.setLevel(saved_level)

    assert len(captured) == 1
    rec = captured[0]
    assert rec.levelno == logging.CRITICAL
    assert rec.exc_info is not None, (
        "the LogRecord must carry exc_info so FileHandler renders the "
        "traceback — without it, only the message string is written"
    )
    assert rec.exc_info[0] is RuntimeError
    assert "crash-payload" in str(rec.exc_info[1])


def test_installed_excepthook_is_idempotent():
    """A repeat install must be a no-op. Without the guard, a caller
    that invoked ``setup_logging`` twice (e.g. a future partner-mode
    worker re-initializing per file) would wrap the hook around
    itself, so a single uncaught exception would log at CRITICAL N
    times where N is the install count."""
    original_hook = sys.excepthook
    try:
        logs_mod._install_logging_excepthook()
        first = sys.excepthook
        logs_mod._install_logging_excepthook()
        assert sys.excepthook is first, (
            "second _install_logging_excepthook call must be a no-op; "
            "wrapping the hook again would produce duplicate CRITICAL "
            "logs per uncaught exception"
        )
    finally:
        sys.excepthook = original_hook


def test_installed_excepthook_delegates_to_previous_hook_in_chain():
    """The install captures ``sys.excepthook`` BEFORE overwriting it,
    so a caller that had already installed an outer hook (pywikibot,
    click, pytest, a debugger) stays in the chain. Delegating to
    ``sys.__excepthook__`` (the untouched default) instead would
    silently discard any such outer wrapper."""
    original_hook = sys.excepthook
    prev_called: list[tuple] = []

    def _prev(exc_type, exc_value, exc_tb):
        prev_called.append((exc_type, exc_value, exc_tb))

    sys.excepthook = _prev
    try:
        logs_mod._install_logging_excepthook()
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            sys.excepthook(*sys.exc_info())
    finally:
        sys.excepthook = original_hook

    assert len(prev_called) == 1, (
        "installed hook must delegate to the previously-installed hook "
        "— otherwise an outer wrapper is silently discarded"
    )
    assert prev_called[0][0] is RuntimeError


def test_installed_excepthook_skips_logging_for_keyboard_interrupt():
    """Ctrl-C is operator-driven, not a crash. Logging it at CRITICAL
    would fill the log with false alarms on every deliberate abort."""
    original_hook = sys.excepthook
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    for h in saved_handlers:
        root.removeHandler(h)
    captured: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record):
            captured.append(record)

    root.addHandler(_Capture(level=logging.DEBUG))
    try:
        logs_mod._install_logging_excepthook()
        try:
            raise KeyboardInterrupt()
        except KeyboardInterrupt:
            sys.excepthook(*sys.exc_info())
    finally:
        sys.excepthook = original_hook
        for h in list(root.handlers):
            root.removeHandler(h)
        for h in saved_handlers:
            root.addHandler(h)

    assert captured == [], (
        "Ctrl-C must not log at CRITICAL — the file would fill with "
        "false alarms on every operator abort"
    )


def test_installed_excepthook_skips_logging_for_system_exit():
    """``sys.exit(0)`` (or any ``SystemExit``) is a clean-exit signal,
    not a crash. Logging every clean exit at CRITICAL would produce
    spurious tracebacks in the log for tools that use ``sys.exit()``
    as a normal control-flow primitive."""
    original_hook = sys.excepthook
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    for h in saved_handlers:
        root.removeHandler(h)
    captured: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record):
            captured.append(record)

    root.addHandler(_Capture(level=logging.DEBUG))
    try:
        logs_mod._install_logging_excepthook()
        try:
            raise SystemExit(0)
        except SystemExit:
            sys.excepthook(*sys.exc_info())
    finally:
        sys.excepthook = original_hook
        for h in list(root.handlers):
            root.removeHandler(h)
        for h in saved_handlers:
            root.addHandler(h)

    assert captured == [], (
        "SystemExit must not log at CRITICAL — every sys.exit() would "
        "otherwise produce a spurious traceback in the log"
    )
