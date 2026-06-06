"""Tests for ingest_wikimedia/ssm.py — the AWS SSM wrapper helper."""

import base64
import re
from unittest.mock import MagicMock

from ingest_wikimedia.ssm import ssm_run, stage_and_launch_tmux


def _make_client_returning(stdout: str) -> MagicMock:
    """Build a mock boto3 SSM client whose send_command + get_command_invocation
    pair returns the given stdout from a Success-status invocation."""
    client = MagicMock()
    client.send_command.return_value = {"Command": {"CommandId": "test-cmd-id"}}
    client.get_command_invocation.return_value = {
        "Status": "Success",
        "StandardOutputContent": stdout,
    }
    # Real boto3 clients expose .exceptions.InvocationDoesNotExist; the
    # production code references it inside an `except` clause. The mock
    # doesn't need a real exception class — set a Sentinel that no real
    # invocation will raise so the polling loop reaches the Success branch.
    client.exceptions.InvocationDoesNotExist = type(
        "_FakeInvocationDoesNotExist", (Exception,), {}
    )
    return client


def test_ssm_run_default_drops_to_ec2_user():
    """Default behavior wraps the command in `sudo -u ec2-user bash -c`."""
    client = _make_client_returning("ok")
    ssm_run(client, "echo hello")
    sent = client.send_command.call_args.kwargs["Parameters"]["commands"][0]
    assert sent.startswith("sudo -u ec2-user bash -c "), sent
    # The original command should appear inside the quoted argument.
    assert "echo hello" in sent


def test_ssm_run_as_root_bypasses_wrapper():
    """`as_root=True` runs the raw command in the SSM root context with no
    `sudo -u ec2-user` wrapper.

    Regression test for PR #228's CodeRabbit finding: the launch script's
    `chown -R ec2-user:ec2-user` heal step needs CAP_CHOWN to fix
    root-owned files, which ec2-user does not have. The previous PR
    revision sent the heal command through the default wrapper and so
    failed with EPERM on every launch.
    """
    client = _make_client_returning("ok")
    ssm_run(client, "chown -R ec2-user:ec2-user /home/ec2-user/repo", as_root=True)
    sent = client.send_command.call_args.kwargs["Parameters"]["commands"][0]
    assert sent == "chown -R ec2-user:ec2-user /home/ec2-user/repo"
    assert "sudo -u ec2-user" not in sent


def test_ssm_run_returns_stripped_stdout():
    """Output is stripped of leading/trailing whitespace."""
    client = _make_client_returning("  hello world  \n")
    result = ssm_run(client, "echo")
    assert result == "hello world"


# ---------------------------------------------------------------------------
# stage_and_launch_tmux: base64-staged pipeline launcher
# ---------------------------------------------------------------------------


def _extract_staged_script(ssm_cmd: str) -> str:
    """Reverse the staging wire format: pull the base64 payload and decode."""
    m = re.search(
        r"echo ([A-Za-z0-9+/=]+) \| base64 -d > /tmp/wm-pipeline-",
        ssm_cmd,
    )
    assert m is not None, f"no base64 stage step found in: {ssm_cmd!r}"
    return base64.b64decode(m.group(1)).decode()


def test_stage_and_launch_tmux_emits_base64_stage_then_tmux_launch():
    """The staged form must (1) base64-decode the script to /tmp, (2)
    chmod +x it, and (3) launch tmux that runs the staged file. All
    three steps need to be present and chained with `&&` so a failure
    at any step short-circuits before the launch."""
    client = _make_client_returning("SESSION_STARTED")
    script = (
        "cd /home/ec2-user/ingest-wikimedia/northwest-heritage && "
        "get-ids-es northwest-heritage --institution 'Foo' > out.csv && "
        "downloader out.csv northwest-heritage"
    )
    stage_and_launch_tmux(
        client,
        script=script,
        session_name="wikimedia-northwest-heritage",
        cwd="/home/ec2-user/ingest-wikimedia/",
    )
    sent = client.send_command.call_args.kwargs["Parameters"]["commands"][0]

    assert "base64 -d > /tmp/wm-pipeline-" in sent, sent
    assert "chmod +x /tmp/wm-pipeline-" in sent, sent
    assert "tmux new-session -d -s" in sent, sent
    assert "'bash /tmp/wm-pipeline-" in sent, sent
    assert "echo SESSION_STARTED" in sent, sent

    # And the decoded script equals exactly what the caller passed in.
    decoded = _extract_staged_script(sent)
    assert decoded == script, f"decoded script does not roundtrip; got: {decoded!r}"


def test_stage_and_launch_tmux_keeps_ssm_payload_small_for_large_scripts():
    """Regression: a 25-target batch (the kind of workload that hit
    SSM's "command too long" limit when serialised inline) must produce
    an SSM payload well within agent limits.

    The inline form would balloon to >25KB once shlex.quote'd through
    bash -c; the staged form should be roughly base64-overhead (~33%)
    of the script size plus a fixed wrapper. Cap the assertion at 50KB
    so we have headroom but still fail loudly if a future refactor
    silently re-introduces the inline payload.
    """
    client = _make_client_returning("SESSION_STARTED")
    # ~25KB script, similar in shape to a 22-target pipeline (sprinkle in
    # single quotes so we'd see the bash-quote-escaping multiplier in the
    # naive form).
    big_script = (
        "cd /home/ec2-user/ingest-wikimedia/foo && "
        "get-ids-es foo --institution 'Some Institution Name' > out.csv\n"
    ) * 200
    assert len(big_script) > 20_000  # ensures we're actually exercising a big payload

    stage_and_launch_tmux(
        client,
        script=big_script,
        session_name="wikimedia-foo",
        cwd="/home/ec2-user/ingest-wikimedia/",
    )
    sent = client.send_command.call_args.kwargs["Parameters"]["commands"][0]

    # Sanity: payload includes the full base64 of the script. Worst case
    # ~4/3 the script size plus the tmux wrapper.
    assert len(sent) < 50_000, (
        f"staged SSM payload unexpectedly large ({len(sent)} bytes) for a "
        f"{len(big_script)}-byte script — staging should keep this well "
        "under SSM's per-command limit."
    )
    # And the decoded script still roundtrips exactly.
    decoded = _extract_staged_script(sent)
    assert decoded == big_script


def test_stage_and_launch_tmux_script_filename_is_deterministic_per_session():
    """Same session_name → same staged-script path. Different session
    names → different paths (so concurrent launches don't collide on
    /tmp)."""
    client = _make_client_returning("SESSION_STARTED")
    stage_and_launch_tmux(client, script="x", session_name="wikimedia-A", cwd="/tmp")
    sent_A1 = client.send_command.call_args.kwargs["Parameters"]["commands"][0]
    stage_and_launch_tmux(client, script="x", session_name="wikimedia-A", cwd="/tmp")
    sent_A2 = client.send_command.call_args.kwargs["Parameters"]["commands"][0]
    stage_and_launch_tmux(client, script="x", session_name="wikimedia-B", cwd="/tmp")
    sent_B = client.send_command.call_args.kwargs["Parameters"]["commands"][0]

    path_re = re.compile(r"/tmp/wm-pipeline-[a-f0-9]+\.sh")
    path_A1 = path_re.search(sent_A1).group(0)
    path_A2 = path_re.search(sent_A2).group(0)
    path_B = path_re.search(sent_B).group(0)

    assert path_A1 == path_A2, (
        f"same session name should produce same script path: {path_A1} vs {path_A2}"
    )
    assert path_A1 != path_B, (
        f"different session names must produce different paths: {path_A1} vs {path_B}"
    )
