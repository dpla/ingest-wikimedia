"""Shared SSM helpers for Wikimedia pipeline scripts."""

import json
import time

INSTANCE_ID = "i-033eff6c8c168f999"
REGION = "us-east-1"
SSM_POLL_INTERVAL = 5
SSM_MAX_POLLS = 60  # 5 minutes


def ssm_run(client, cmd: str) -> str:
    resp = client.send_command(
        InstanceIds=[INSTANCE_ID],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [f"sudo -u ec2-user bash -c {json.dumps(cmd)}"]},
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
