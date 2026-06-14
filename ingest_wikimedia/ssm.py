"""Shared SSM helpers for Wikimedia pipeline scripts."""

import base64
import hashlib
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


def fetch_memory_snapshot(client) -> tuple[int, int] | None:
    """Return ``(total_mb, available_mb)`` from ``free -m`` on the
    instance, or ``None`` if the command failed or its output couldn't
    be parsed.

    Callers shape the numbers for their own context — ``wikimedia_launch``
    /``wikimedia_retry`` gate their startup on a headroom percentage,
    while ``wikimedia_upload_status`` formats it as a Slack readout —
    so this helper deliberately returns the raw pair rather than a
    formatted string or a derived percentage. Centralising the shell
    command + parsing here keeps the three call sites from drifting on
    output handling (e.g., the divide-by-zero guard that landed here
    but isn't in the older inline copies).
    """
    import logging

    try:
        raw = ssm_run(client, "free -m | awk 'NR==2{print $2, $7}'")
    except Exception:
        logging.exception("Failed to fetch instance memory snapshot")
        return None
    parts = raw.split()
    if len(parts) != 2:
        logging.warning("Unexpected free -m output: %r", raw)
        return None
    try:
        total_mb, available_mb = int(parts[0]), int(parts[1])
    except ValueError:
        logging.warning("Could not parse free -m output: %r", raw)
        return None
    if total_mb <= 0:
        logging.warning("Non-positive total memory in free -m output: %r", raw)
        return None
    return total_mb, available_mb


def stage_and_launch_tmux(client, *, script: str, session_name: str, cwd: str) -> str:
    """Stage `script` to a file on the instance, then launch a detached
    tmux session that runs it. Returns whatever ssm_run returns (stdout).

    Use this instead of an inline ``tmux new-session ... "PIPELINE_CMD"``
    SSM call when ``script`` may be long.  SSM's per-command size limit
    (the agent rejects with "command too long" once exceeded — observed
    on a 22-target batch of ~25KB) bites when long batch pipelines or
    institution-rich target lists get serialised inline, especially
    once shlex.quote'ing many embedded single quotes inflates them
    further.  Base64 adds only ~33% overhead vs the 2-3x growth from
    quote-escaping, and the base64 alphabet (``[A-Za-z0-9+/=]``) has no
    characters that need shell escaping, so the SSM payload stays
    compact regardless of script content.

    Side benefit: the staged script runs in a fresh bash via
    ``bash <path>``, so its variable references see raw ``$?`` / ``$rc``
    / ``$0`` etc. without needing backslash escapes that were only
    necessary when the pipeline was embedded as a double-quoted
    argument to an outer bash through tmux.  Callers should pass the
    raw, unescaped script form.

    The script filename is derived from a SHA-1 of ``session_name`` so
    two concurrent launches with different sessions don't collide on
    ``/tmp``, but the same launch retried after a transient error
    overwrites cleanly.
    """
    script_id = hashlib.sha1(session_name.encode()).hexdigest()[:12]
    script_path = f"/tmp/wm-pipeline-{script_id}.sh"
    script_b64 = base64.b64encode(script.encode()).decode()
    staged_cmd = (
        f"echo {script_b64} | base64 -d > {script_path} && "
        f"chmod +x {script_path} && "
        f"tmux new-session -d -s {shlex.quote(session_name)} "
        f"-c {shlex.quote(cwd)} 'bash {script_path}' && "
        "echo SESSION_STARTED"
    )
    return ssm_run(client, staged_cmd)
