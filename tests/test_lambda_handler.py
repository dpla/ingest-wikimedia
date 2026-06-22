"""
Tests for the Slack slash-command Lambda handler.

The handler lives outside the importable package (in ``lambda/...``), so the
tests load it via ``importlib`` rather than a normal ``import`` statement.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import urllib.parse
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
HANDLER_PATH = REPO_ROOT / "lambda" / "wikimedia-slack-dispatch" / "handler.py"


@pytest.fixture(scope="module")
def handler_module():
    # Ensure the package import inside the handler resolves to the repo's
    # ingest_wikimedia, not anything installed globally.
    sys.path.insert(0, str(REPO_ROOT))
    spec = importlib.util.spec_from_file_location(
        "slack_dispatch_handler", HANDLER_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _make_event(text: str) -> dict:
    body = urllib.parse.urlencode(
        {
            "command": "/wikimedia-upload",
            "text": text,
            "response_url": "https://hooks.slack.example/response",
        }
    )
    return {
        "headers": {
            "x-slack-request-timestamp": "0",
            "x-slack-signature": "v0=stub",
        },
        "body": body,
    }


def _setup_env_and_stubs(monkeypatch, handler_module, dispatched: list[dict]):
    monkeypatch.setenv("SLACK_SIGNING_SECRET", "shh")
    monkeypatch.setenv("GH_TOKEN", "tok")
    monkeypatch.setattr(
        handler_module, "_verify_slack_signature", lambda *_a, **_k: True
    )

    def fake_dispatch(token, repo, workflow, inputs):
        dispatched.append({"workflow": workflow, "inputs": inputs})
        return 204

    monkeypatch.setattr(handler_module, "_dispatch_workflow", fake_dispatch)


def _decode_reply(reply: dict) -> dict:
    return json.loads(reply["body"])


def test_sdc_subcommand_dispatches_launch_with_sdc_only_true(
    monkeypatch, handler_module
):
    dispatched: list[dict] = []
    _setup_env_and_stubs(monkeypatch, handler_module, dispatched)

    reply = handler_module.handler(
        _make_event('sdc "nara|William J. Clinton Library"'), None
    )

    assert reply["statusCode"] == 200
    assert len(dispatched) == 1
    call = dispatched[0]
    assert call["workflow"] == "wikimedia-launch.yml"
    assert call["inputs"]["sdc_only"] == "true"
    # SDC-only must never set refresh_only (mutually exclusive in wikimedia_launch.py).
    assert "refresh_only" not in call["inputs"]
    assert call["inputs"]["partner"] == "'nara|William J. Clinton Library'"
    # concurrency_key is required so SDC + regular launch queue against each
    # other; ``_launch_with_targets`` derives it from the partner string.
    assert len(call["inputs"]["concurrency_key"]) == 16
    assert "SDC-only sync" in _decode_reply(reply)["text"]


def test_sdc_subcommand_supports_multiple_targets(monkeypatch, handler_module):
    dispatched: list[dict] = []
    _setup_env_and_stubs(monkeypatch, handler_module, dispatched)

    reply = handler_module.handler(
        _make_event(
            'sdc "nara|William J. Clinton Library" "nara|John F. Kennedy Library"'
        ),
        None,
    )

    assert reply["statusCode"] == 200
    assert len(dispatched) == 1
    partner = dispatched[0]["inputs"]["partner"]
    # shlex.join re-quotes targets containing spaces — both must survive.
    assert "William J. Clinton Library" in partner
    assert "John F. Kennedy Library" in partner


def test_sdc_subcommand_with_no_targets_returns_usage(monkeypatch, handler_module):
    dispatched: list[dict] = []
    _setup_env_and_stubs(monkeypatch, handler_module, dispatched)

    reply = handler_module.handler(_make_event("sdc"), None)

    assert reply["statusCode"] == 200
    assert dispatched == []  # no workflow dispatched on usage-error replies
    text = _decode_reply(reply)["text"]
    assert "Usage:" in text
    assert "/wikimedia-upload sdc" in text


def test_sdc_subcommand_rejects_unknown_hub(monkeypatch, handler_module):
    dispatched: list[dict] = []
    _setup_env_and_stubs(monkeypatch, handler_module, dispatched)

    reply = handler_module.handler(_make_event("sdc not-a-real-hub"), None)

    assert reply["statusCode"] == 200
    assert dispatched == []
    assert "Unknown hub" in _decode_reply(reply)["text"]


def test_top_level_usage_mentions_sdc_subcommand(monkeypatch, handler_module):
    dispatched: list[dict] = []
    _setup_env_and_stubs(monkeypatch, handler_module, dispatched)

    reply = handler_module.handler(_make_event(""), None)

    assert reply["statusCode"] == 200
    text = _decode_reply(reply)["text"]
    assert "/wikimedia-upload sdc" in text
    assert "/wikimedia-upload maintain" in text


def test_dispatch_helper_treats_raw_timeout_as_possibly_dispatched(
    monkeypatch, handler_module
):
    """Bare ``TimeoutError`` must not surface as 'internal error'.

    Kept as a backstop even though ``urllib.request.urlopen`` normally wraps
    timeouts in ``URLError`` — defensive in case a future urllib change (or
    a different code path) propagates the raw exception.
    """
    monkeypatch.setenv("SLACK_SIGNING_SECRET", "shh")
    monkeypatch.setenv("GH_TOKEN", "tok")
    monkeypatch.setattr(
        handler_module, "_verify_slack_signature", lambda *_a, **_k: True
    )

    def slow_dispatch(*_a, **_k):
        raise TimeoutError("simulated slow GitHub API")

    monkeypatch.setattr(handler_module, "_dispatch_workflow", slow_dispatch)

    reply = handler_module.handler(
        _make_event('sdc "nara|William J. Clinton Library"'), None
    )

    assert reply["statusCode"] == 200
    text = _decode_reply(reply)["text"]
    assert "internal error" not in text.lower()
    assert "may have been dispatched" in text
    assert "#tech-alerts" in text


def test_dispatch_helper_treats_urlerror_wrapping_timeout_as_possibly_dispatched(
    monkeypatch, handler_module
):
    """The realistic urllib timeout shape: ``URLError`` wrapping ``TimeoutError``.

    ``urllib.request.urlopen()`` does not propagate a raw ``TimeoutError`` from
    its ``timeout=`` parameter — it wraps the underlying ``socket.timeout``
    (aliased to ``TimeoutError`` in Python 3.10+) inside ``URLError.reason``.
    This is the shape that actually hits the Lambda in production, so the
    timeout branch must recognise it and route to the softer message.
    """
    import urllib.error

    monkeypatch.setenv("SLACK_SIGNING_SECRET", "shh")
    monkeypatch.setenv("GH_TOKEN", "tok")
    monkeypatch.setattr(
        handler_module, "_verify_slack_signature", lambda *_a, **_k: True
    )

    def wrapped_timeout_dispatch(*_a, **_k):
        raise urllib.error.URLError(TimeoutError("timed out"))

    monkeypatch.setattr(handler_module, "_dispatch_workflow", wrapped_timeout_dispatch)

    reply = handler_module.handler(
        _make_event('sdc "nara|William J. Clinton Library"'), None
    )

    assert reply["statusCode"] == 200
    text = _decode_reply(reply)["text"]
    assert "internal error" not in text.lower()
    assert "may have been dispatched" in text
    assert "#tech-alerts" in text


def test_dispatch_helper_non_timeout_urlerror_is_hard_failure(
    monkeypatch, handler_module
):
    """A non-timeout URLError (DNS, connection refused) must NOT use the soft message.

    Only timeout-shaped URLErrors leave the dispatch in ambiguous-status
    territory. A connection refused or DNS failure means the request never
    arrived, so the hard-failure wording is correct.
    """
    import urllib.error

    monkeypatch.setenv("SLACK_SIGNING_SECRET", "shh")
    monkeypatch.setenv("GH_TOKEN", "tok")
    monkeypatch.setattr(
        handler_module, "_verify_slack_signature", lambda *_a, **_k: True
    )

    def refused_dispatch(*_a, **_k):
        raise urllib.error.URLError(ConnectionRefusedError("connection refused"))

    monkeypatch.setattr(handler_module, "_dispatch_workflow", refused_dispatch)

    reply = handler_module.handler(
        _make_event('sdc "nara|William J. Clinton Library"'), None
    )

    assert reply["statusCode"] == 200
    text = _decode_reply(reply)["text"]
    assert "may have been dispatched" not in text
    assert "internal error" in text.lower()


def test_maintain_subcommand_dispatches_launch_with_maintain_true(
    monkeypatch, handler_module
):
    dispatched: list[dict] = []
    _setup_env_and_stubs(monkeypatch, handler_module, dispatched)

    reply = handler_module.handler(_make_event("maintain digitalnc"), None)

    assert reply["statusCode"] == 200
    assert len(dispatched) == 1
    call = dispatched[0]
    assert call["workflow"] == "wikimedia-launch.yml"
    assert call["inputs"]["maintain"] == "true"
    # Maintain is its own run mode — must never set the others.
    assert "sdc_only" not in call["inputs"]
    assert "refresh_only" not in call["inputs"]
    assert call["inputs"]["partner"] == "digitalnc"
    assert len(call["inputs"]["concurrency_key"]) == 16
    assert "maintain" in _decode_reply(reply)["text"].lower()


def test_maintain_subcommand_with_no_targets_returns_usage(monkeypatch, handler_module):
    dispatched: list[dict] = []
    _setup_env_and_stubs(monkeypatch, handler_module, dispatched)

    reply = handler_module.handler(_make_event("maintain"), None)

    assert reply["statusCode"] == 200
    assert dispatched == []
    text = _decode_reply(reply)["text"]
    assert "Usage:" in text
    assert "/wikimedia-upload maintain" in text


def test_maintain_count_subcommand_dispatches_with_count_only_true(
    monkeypatch, handler_module
):
    dispatched: list[dict] = []
    _setup_env_and_stubs(monkeypatch, handler_module, dispatched)

    reply = handler_module.handler(_make_event("maintain count digitalnc"), None)

    assert reply["statusCode"] == 200
    assert len(dispatched) == 1
    inputs = dispatched[0]["inputs"]
    assert inputs["maintain"] == "true"
    assert inputs["count_only"] == "true"
    assert inputs["partner"] == "digitalnc"
    assert "sizing" in _decode_reply(reply)["text"].lower()


def test_maintain_count_with_no_targets_returns_usage(monkeypatch, handler_module):
    dispatched: list[dict] = []
    _setup_env_and_stubs(monkeypatch, handler_module, dispatched)

    reply = handler_module.handler(_make_event("maintain count"), None)

    assert reply["statusCode"] == 200
    assert dispatched == []
    assert "Usage:" in _decode_reply(reply)["text"]


def test_plain_maintain_does_not_set_count_only(monkeypatch, handler_module):
    dispatched: list[dict] = []
    _setup_env_and_stubs(monkeypatch, handler_module, dispatched)

    reply = handler_module.handler(_make_event("maintain digitalnc"), None)

    assert reply["statusCode"] == 200
    assert "count_only" not in dispatched[0]["inputs"]
