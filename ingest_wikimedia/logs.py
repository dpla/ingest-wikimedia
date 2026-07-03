import logging
import os
import sys
from datetime import datetime

from tqdm import tqdm


def _install_logging_excepthook() -> None:
    """Replace ``sys.excepthook`` with a wrapper that logs the
    exception via ``logging.critical(exc_info=...)`` before delegating
    to whatever hook was previously installed.

    Uncaught exceptions otherwise write only to ``sys.stderr``. Tools
    running under tmux lose stderr when the session ends, leaving the
    file logger (populated by :func:`setup_logging`) as the only
    surviving artifact — but the default hook never routes through
    that logger, so the file log stops at whatever logged just before
    the crash.

    The previous hook is captured (not ``sys.__excepthook__``) so an
    outer wrapper — pywikibot, click, pytest, a debugger — stays in
    the chain rather than being silently discarded.

    ``KeyboardInterrupt`` and ``SystemExit`` skip the logging branch:
    they're operator-driven / clean-exit signals, not crashes, and
    logging them at CRITICAL would fill the log with false alarms on
    every ``Ctrl-C`` or ``sys.exit(0)``.

    Idempotent: a repeat install is a no-op. Without this a caller that
    invoked ``setup_logging`` twice would wrap the hook around itself,
    logging every uncaught exception N times where N is the install
    count. No production caller does this today, but the guard keeps
    the behavior safe against future reuse (e.g. a partner-mode worker
    re-initializing per file).
    """
    if getattr(sys.excepthook, "_is_logging_excepthook", False):
        return
    prev_hook = sys.excepthook

    def _hook(exc_type, exc_value, exc_tb):
        if issubclass(exc_type, (KeyboardInterrupt, SystemExit)):
            prev_hook(exc_type, exc_value, exc_tb)
            return
        logging.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_tb))
        prev_hook(exc_type, exc_value, exc_tb)

    _hook._is_logging_excepthook = True
    sys.excepthook = _hook


class TqdmLoggingHandler(logging.Handler):
    """
    This class redirects logging's console output through tqdm so the progress
    bars don't get mangled.
    """

    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except (IOError, OSError):
            self.handleError(record)


def setup_logging(partner: str, event_type: str, level: int = logging.INFO) -> None:
    """
    Creates a logfile for this process with a unique timestamp and with the partner's
    name. Passes local logging through tqdm so the progress bars don't get mangled.
    Suppresses pywikibot logging below ERROR. Installs a
    logging-aware ``sys.excepthook`` so uncaught tracebacks reach the
    log file — see :func:`_install_logging_excepthook`.
    """
    os.makedirs(LOGS_DIR_BASE, exist_ok=True)
    time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
    session_label = os.environ.get("WIKIMEDIA_SESSION_LABEL") or partner
    log_file_name = f"{time_str}-{session_label}-{event_type}.log"
    filename = f"{LOGS_DIR_BASE}/{log_file_name}"
    logging.basicConfig(
        level=level,
        datefmt="%H:%M:%S",
        handlers=[
            TqdmLoggingHandler(),
            logging.FileHandler(filename=filename, mode="w"),
        ],
        format="[%(levelname)s] %(asctime)s: %(message)s",
    )
    logging.info(f"Logging to {filename}.")
    for d in logging.Logger.manager.loggerDict:
        if d.startswith("pywiki"):
            logging.getLogger(d).setLevel(level)
    _install_logging_excepthook()


LOGS_DIR_BASE = "./logs"
