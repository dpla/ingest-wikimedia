"""
Lambda handler for the /wikimedia-status Slack slash command.

Validates the incoming Slack request signature, dispatches the
wikimedia-upload-status GitHub Actions workflow, and returns an
immediate acknowledgment to Slack (results post to #tech-alerts
once the workflow completes, typically ~2 minutes later).

Environment variables (set on the Lambda function):
  SLACK_SIGNING_SECRET  — from Slack app Basic Information page
  GH_TOKEN              — GitHub fine-grained PAT with actions:write
  GH_REPO               — e.g. dpla/ingest-wikimedia
  GH_WORKFLOW           — e.g. wikimedia-upload-status.yml
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import time
import urllib.error
import urllib.request


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


def _dispatch_workflow(token: str, repo: str, workflow: str) -> int:
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches"
    req = urllib.request.Request(
        url,
        data=json.dumps({"ref": "main"}).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status


def handler(event, context):
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")

    timestamp = headers.get("x-slack-request-timestamp", "")
    signature = headers.get("x-slack-signature", "")

    if not timestamp or not signature:
        return {"statusCode": 400, "body": "Missing Slack headers"}

    if not _verify_slack_signature(
        os.environ["SLACK_SIGNING_SECRET"], timestamp, body, signature
    ):
        return {"statusCode": 401, "body": "Invalid signature"}

    repo = os.environ.get("GH_REPO", "dpla/ingest-wikimedia")
    workflow = os.environ.get("GH_WORKFLOW", "wikimedia-upload-status.yml")

    try:
        status = _dispatch_workflow(os.environ["GH_TOKEN"], repo, workflow)
    except urllib.error.HTTPError as e:
        logging.error("GitHub API error: HTTP %s", e.code)
        msg = f"Failed to trigger workflow (HTTP {e.code})"
    except Exception:
        logging.exception("Unexpected error dispatching workflow")
        msg = "Failed to trigger workflow due to an internal error."
    else:
        msg = (
            "Checking Wikimedia upload status — results will post to #tech-alerts shortly."
            if status == 204
            else f"Unexpected response from GitHub (HTTP {status})"
        )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"response_type": "in_channel", "text": msg}),
    }
