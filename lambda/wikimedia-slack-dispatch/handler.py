"""
Lambda handler for Wikimedia Slack slash commands.

Handles:
  /wikimedia-status  — dispatches wikimedia-upload-status.yml; results post to
                       #tech-alerts once the workflow completes (~2 minutes).
  /wikimedia-upload <target> [<target> ...]
                     — dispatches wikimedia-launch.yml; one tmux session runs all
                       targets sequentially. Each target is a hub slug ("bpl") or
                       a hub|institution pair ("indiana|Indiana State Library").
  /wikimedia-upload kill <hub> [<hub> ...]
                     — dispatches wikimedia-kill.yml to stop running sessions.

Validates the incoming Slack request signature before dispatching.

Environment variables (set on the Lambda function):
  SLACK_SIGNING_SECRET  — from Slack app Basic Information page
  GH_TOKEN              — GitHub fine-grained PAT with actions:write on dpla/ingest-wikimedia
  GH_REPO               — e.g. dpla/ingest-wikimedia (optional, has default)
"""

import base64
import binascii
import hashlib
import hmac
import json
import logging
import os
import re
import shlex
import time
import urllib.error
import urllib.parse
import urllib.request

from ingest_wikimedia.partners import resolve_slug

_QID_RE = re.compile(r"^Q\d+$")


def _verify_slack_signature(
    signing_secret: str, timestamp: str, body: str, signature: str
) -> bool:
    try:
        ts = int(timestamp)
    except (TypeError, ValueError):
        return False
    if abs(time.time() - ts) > 300:
        return False
    sig_base = f"v0:{timestamp}:{body}".encode()
    mac = hmac.new(signing_secret.encode(), sig_base, hashlib.sha256)
    return hmac.compare_digest("v0=" + mac.hexdigest(), signature)


def _require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        logging.error("Missing required environment variable: %s", key)
        raise RuntimeError(key)
    return value


def _dispatch_workflow(token: str, repo: str, workflow: str, inputs: dict) -> int:
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches"
    req = urllib.request.Request(
        url,
        data=json.dumps({"ref": "main", "inputs": inputs}).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=2) as resp:
        return resp.status


def _slack_reply(text: str, ephemeral: bool = False) -> dict:
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(
            {
                "response_type": "ephemeral" if ephemeral else "in_channel",
                "text": text,
            }
        ),
    }


def _dispatch_and_reply(
    token: str, repo: str, workflow: str, inputs: dict, success_text: str
) -> dict:
    try:
        status = _dispatch_workflow(token, repo, workflow, inputs)
    except urllib.error.HTTPError as e:
        logging.error("GitHub API error: HTTP %s", e.code)
        return _slack_reply(f"Failed to trigger workflow (HTTP {e.code}).")
    except Exception:
        logging.exception("Unexpected error dispatching workflow")
        return _slack_reply("Failed to trigger workflow due to an internal error.")
    text = (
        success_text
        if status == 204
        else f"Unexpected response from GitHub (HTTP {status})"
    )
    return _slack_reply(text)


def handler(event, context):
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        try:
            body = base64.b64decode(body).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError):
            return {"statusCode": 400, "body": "Invalid request body encoding"}

    timestamp = headers.get("x-slack-request-timestamp", "")
    signature = headers.get("x-slack-signature", "")

    if not timestamp or not signature:
        return {"statusCode": 400, "body": "Missing Slack headers"}

    try:
        signing_secret = _require_env("SLACK_SIGNING_SECRET")
        gh_token = _require_env("GH_TOKEN")
    except RuntimeError:
        return {"statusCode": 500, "body": "Server misconfiguration"}

    if not _verify_slack_signature(signing_secret, timestamp, body, signature):
        return {"statusCode": 401, "body": "Invalid signature"}

    repo = os.environ.get("GH_REPO", "dpla/ingest-wikimedia")
    fields = dict(urllib.parse.parse_qsl(body))
    command = fields.get("command", "")

    if command == "/wikimedia-status":
        try:
            status = _dispatch_workflow(
                gh_token,
                repo,
                "wikimedia-upload-status.yml",
                {"notify_if_idle": "true"},
            )
        except urllib.error.HTTPError as e:
            logging.error("GitHub API error: HTTP %s", e.code)
            return _slack_reply(f"Failed to trigger workflow (HTTP {e.code}).")
        except TimeoutError:
            logging.warning(
                "Timeout waiting for GitHub dispatch response (wikimedia-upload-status)"
            )
            return _slack_reply(
                "GitHub API was slow — status check may have been dispatched anyway. "
                "Watch #tech-alerts for results or try again."
            )
        except Exception:
            logging.exception("Unexpected error dispatching workflow")
            return _slack_reply("Failed to trigger workflow due to an internal error.")
        text = (
            "Checking Wikimedia upload status — results will post to #tech-alerts shortly."
            if status == 204
            else f"Unexpected response from GitHub (HTTP {status})"
        )
        return _slack_reply(text)

    if command == "/wikimedia-upload":
        raw = fields.get("text", "").strip()
        if not raw:
            return _slack_reply(
                "Usage: `/wikimedia-upload <hub> [<hub> ...]` or"
                " `/wikimedia-upload <hub>|<institution>` or"
                " `/wikimedia-upload kill <hub> [<hub> ...]`",
                ephemeral=True,
            )

        try:
            tokens = shlex.split(raw)
        except ValueError as e:
            return _slack_reply(f"Could not parse command: {e}", ephemeral=True)

        response_url = fields.get("response_url", "")

        # Kill subcommand: /wikimedia-upload kill <label> [<label> ...]
        # Targets are session label suffixes as shown by /wikimedia-status
        # (e.g. "bpl", "indiana-state-library"). QIDs are also accepted.
        if tokens[0] == "kill":
            kill_targets = tokens[1:]
            if not kill_targets:
                return _slack_reply(
                    "Usage: `/wikimedia-upload kill <label> [<label> ...]`"
                    " — use the session label from `/wikimedia-status`",
                    ephemeral=True,
                )
            partner_input = shlex.join(kill_targets)
            label = ", ".join(f"`{t}`" for t in kill_targets)
            return _dispatch_and_reply(
                gh_token,
                repo,
                "wikimedia-kill.yml",
                {"partner": partner_input, "response_url": response_url},
                f"Kill signal sent for {label} — result will post to #tech-alerts shortly.",
            )

        # Launch subcommand: /wikimedia-upload <target> [<target> ...]
        # QIDs are passed through; the launch script resolves them.
        # Dict preserves insertion order for stable session naming.
        seen_tokens: set[str] = set()
        launch_targets: list[str] = []
        for token in tokens:
            if _QID_RE.match(token):
                # Wikidata QID — validate format, pass through unchanged.
                if token in seen_tokens:
                    return _slack_reply(
                        f"Target `{token}` appears more than once.",
                        ephemeral=True,
                    )
                seen_tokens.add(token)
                launch_targets.append(token)
            else:
                hub_part, institution = (
                    token.split("|", 1) if "|" in token else (token, None)
                )
                canonical = resolve_slug(hub_part)
                if canonical is None:
                    return _slack_reply(
                        f"Unknown hub: `{hub_part}`. Check the hub slug and try again.",
                        ephemeral=True,
                    )
                if canonical == "nara":
                    return _slack_reply(
                        "NARA requires a separate process and cannot be launched here.",
                        ephemeral=True,
                    )
                target_str = f"{canonical}|{institution}" if institution else canonical
                if target_str in seen_tokens:
                    return _slack_reply(
                        f"Target `{target_str}` appears more than once.",
                        ephemeral=True,
                    )
                seen_tokens.add(target_str)
                launch_targets.append(target_str)

        partner_input = shlex.join(launch_targets)
        targets_display = ", ".join(f"`{t}`" for t in launch_targets)

        try:
            status = _dispatch_workflow(
                gh_token,
                repo,
                "wikimedia-launch.yml",
                {"partner": partner_input, "response_url": response_url},
            )
        except urllib.error.HTTPError as e:
            logging.error("GitHub API error: HTTP %s", e.code)
            return _slack_reply(
                f"Failed to launch pipeline for {targets_display} (HTTP {e.code})."
            )
        except TimeoutError:
            logging.warning(
                "Timeout waiting for GitHub dispatch response for targets: %s",
                targets_display,
            )
            return _slack_reply(
                f"GitHub API was slow — pipeline for {targets_display} may have been dispatched anyway. "
                "Check #tech-alerts in ~2 minutes or the Actions tab to confirm."
            )
        except Exception:
            logging.exception("Unexpected error dispatching workflow")
            return _slack_reply(
                f"Failed to launch pipeline for {targets_display} due to an internal error."
            )
        text = (
            f"Launching pipeline for {targets_display} — confirmation will post to #tech-alerts shortly."
            if status == 204
            else f"Unexpected response from GitHub (HTTP {status})"
        )
        return _slack_reply(text)

    return {"statusCode": 400, "body": f"Unknown command: {command}"}
