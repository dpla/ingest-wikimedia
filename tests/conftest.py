"""Test-suite-wide safety net for Slack-side leakage.

Several scripts in this repo post to the DPLA bot's Slack channel when
they encounter operational failures (``_slack_fail``'s fallback
``post_message`` call in ``wikimedia_launch.py``) or when their no-op
path runs (``post_message`` from the "No retryable failures found" path
in ``wikimedia_retry.py``). When ``DPLA_SLACK_BOT_TOKEN`` is set in the
environment (typical of EC2 where ``.bashrc`` exports it), an unmocked
test that exercises these code paths posts a real message to
``#tech-alerts``.

Most tests do mock ``post_message`` locally, but missing a single mock
on a single new test silently leaks. This autouse fixture removes the
class of bug by:

  1. Stripping every Slack token / webhook env var before each test, so
     code paths gated on ``if os.environ.get('DPLA_SLACK_BOT_TOKEN')``
     short-circuit cleanly.
  2. Patching every ``post_message`` import site to a no-op so even
     code that doesn't gate on the env var (or that captured the token
     at import time) can't reach Slack.

A test that genuinely needs ``post_message`` to be exercised — e.g. an
assertion on the message text — can still install its own ``patch.object``
and assert against it, since the test's local patch is the inner-most
context manager and wins over this autouse stub.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest


_SLACK_ENV_VARS = (
    "DPLA_SLACK_BOT_TOKEN",
    "DPLA_SLACK_WEBHOOK",
    "DPLA_SLACK_INGESTS_BOT_TOKEN",
)

# Every module that imports ``post_message`` directly. Each import site
# gets its own module-level binding, so patching just
# ``ingest_wikimedia.slack.post_message`` doesn't reach the call sites
# in ``scripts/*`` that did ``from ingest_wikimedia.slack import
# post_message`` at module load. Mirror the list of importers
# discovered via ``grep -rn "from ingest_wikimedia.slack import"``.
_POST_MESSAGE_IMPORT_SITES = (
    "ingest_wikimedia.slack.post_message",
    "scripts.wikimedia_launch.post_message",
    "scripts.wikimedia_retry.post_message",
    "scripts.wikimedia_kill.post_message",
    "scripts.wikimedia_upload_status.post_message",
)


@pytest.fixture(autouse=True)
def _no_slack_in_tests(monkeypatch: pytest.MonkeyPatch):
    """Autouse fixture preventing any test from posting to Slack.

    Runs before every test; teardown automatic via monkeypatch / patch.
    """
    for var in _SLACK_ENV_VARS:
        monkeypatch.delenv(var, raising=False)

    patches = []
    for target in _POST_MESSAGE_IMPORT_SITES:
        try:
            p = patch(target)
            p.start()
            patches.append(p)
        except (AttributeError, ModuleNotFoundError):
            # Module exists in the import graph but doesn't bind
            # post_message at module level (or doesn't exist at all in
            # this test run). Skip — the env-var strip above is the
            # primary defense; this patch list is belt-and-suspenders.
            continue

    yield

    # Best-effort cleanup: if one patch's stop() raises (rare but possible
    # when a test has already manually stopped it), keep going so the
    # remaining patches still get torn down.  A stranded patch would leak
    # into subsequent tests and surface as confusing test-order
    # dependencies.
    for p in patches:
        try:
            p.stop()
        except Exception:
            # Best-effort: the patch may have already been stopped (e.g.
            # by a test that manually managed its lifetime).  Swallow so
            # subsequent ``p.stop()`` calls in this loop still run.
            pass
