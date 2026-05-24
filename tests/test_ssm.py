"""Tests for ingest_wikimedia/ssm.py — the AWS SSM wrapper helper."""

from unittest.mock import MagicMock

from ingest_wikimedia.ssm import ssm_run


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
