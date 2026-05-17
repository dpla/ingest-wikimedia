"""
Lambda handler for Wikimedia Slack slash commands.

Handles:
  /wikimedia-status  — dispatches wikimedia-upload-status.yml; results post to
                       #tech-alerts once the workflow completes (~2 minutes).
  /wikimedia-upload <target> [<target> ...]
                     — dispatches wikimedia-launch.yml; one tmux session runs all
                       targets sequentially. Each target is one of:
                         hub slug            e.g. "bpl"
                         hub|institution     e.g. "indiana|Indiana State Library"
                         hub|institution|collection
                         DPLA item ID        e.g. "087a554ba5d8feb82b1d9c26380d7d0f"
                         Wikidata QID        e.g. "Q12345"
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

from ingest_wikimedia.partners import is_dpla_id, resolve_slug

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
                "Usage: `/wikimedia-upload <target> [<target> ...]`\n"
                "Targets: hub slug, `hub|institution`, `hub|institution|collection`,"
                " DPLA item ID, or Wikidata QID.\n"
                "Wrap targets containing spaces in quotes:"
                ' `/wikimedia-upload "indiana|Indiana State Library"`\n'
                "To stop a session: `/wikimedia-upload kill <label> [<label> ...]`",
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
        # QIDs and DPLA IDs are passed through; the launch script resolves them.
        # Hub-based targets are validated here for fast Slack feedback.
        seen_tokens: set[str] = set()
        launch_targets: list[str] = []
        for token in tokens:
            if _QID_RE.match(token):
                # Wikidata QID — pass through unchanged.
                if token in seen_tokens:
                    return _slack_reply(
                        f"Target `{token}` appears more than once.",
                        ephemeral=True,
                    )
                seen_tokens.add(token)
                launch_targets.append(token)
            elif is_dpla_id(token):
                # DPLA item ID — normalise to lowercase and pass through for
                # resolution by the launch script on EC2.
                normalised = token.lower()
                if normalised in seen_tokens:
                    return _slack_reply(
                        f"Target `{normalised}` appears more than once.",
                        ephemeral=True,
                    )
                seen_tokens.add(normalised)
                launch_targets.append(normalised)
            else:
                pipe_count = token.count("|")
                if pipe_count > 2:
                    return _slack_reply(
                        f"Invalid target: `{token}`."
                        " Use `<hub>`, `<hub>|<institution>`,"
                        " or `<hub>|<institution>|<collection>`.",
                        ephemeral=True,
                    )
                parts = token.split("|", 2)
                hub_part = parts[0].strip()
                institution = parts[1].strip() if pipe_count >= 1 else None
                collection = parts[2].strip() if pipe_count >= 2 else None
                if institution is not None and not institution:
                    return _slack_reply("Institution cannot be empty.", ephemeral=True)
                if collection is not None and not collection:
                    return _slack_reply("Collection cannot be empty.", ephemeral=True)
                canonical = resolve_slug(hub_part)
                if canonical is None:
                    return _slack_reply(
                        f"Unknown hub: `{hub_part}`. Check the hub slug and try again.",
                        ephemeral=True,
                    )
                if collection is not None:
                    target_str = f"{canonical}|{institution}|{collection}"
                elif institution is not None:
                    target_str = f"{canonical}|{institution}"
                else:
                    target_str = canonical
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
