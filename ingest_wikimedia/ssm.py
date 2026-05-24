"""Shared SSM helpers for Wikimedia pipeline scripts."""

import shlex
import time

INSTANCE_ID = "i-033eff6c8c168f999"
REGION = "us-east-1"
SSM_POLL_INTERVAL = 5
SSM_MAX_POLLS = 60  # 5 minutes


def ssm_run(client, cmd: str, *, as_root: bool = False) -> str:
    """Run cmd on EC2 via AWS-RunShellScript SSM, default ec2-user context.

    The AWS-RunShellScript document executes as root by default. Most
    pipeline commands need to act on the ec2-user-owned working tree and
    venv, so by default we wrap the command in `sudo -u ec2-user bash -c`.

    Pass as_root=True to bypass the wrapper and run with full root
    privilege — needed for the rare operations only root can do, such as
    `chown` of files owned by a different user. Callers that need root
    should be explicit about it; the default keeps file ownership
    consistently under ec2-user.
    """
    if as_root:
        wrapped = cmd
    else:
        wrapped = f"sudo -u ec2-user bash -c {shlex.quote(cmd)}"
    resp = client.send_command(
        InstanceIds=[INSTANCE_ID],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [wrapped]},
    )
    cmd_id = resp["Command"]["CommandId"]
    for attempt in range(SSM_MAX_POLLS):
        if attempt > 0:
            time.sleep(SSM_POLL_INTERVAL)
        try:
            inv = client.get_command_invocation(
                CommandId=cmd_id, InstanceId=INSTANCE_ID
            )
        except client.exceptions.InvocationDoesNotExist:
            continue
        status = inv["Status"]
        if status == "Success":
            return inv.get("StandardOutputContent", "").strip()
        if status in ("Failed", "TimedOut", "Cancelled"):
            stderr = inv.get("StandardErrorContent", "").strip()
            raise RuntimeError(
                f"SSM command {cmd_id} ended with {status}: {stderr or 'no stderr'}"
            )
    raise TimeoutError(f"SSM command {cmd_id} did not complete within polling window")
