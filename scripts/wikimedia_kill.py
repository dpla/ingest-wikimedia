#!/usr/bin/env python3
"""Kill a running Wikimedia upload pipeline session on EC2.

Finds all tmux sessions whose name contains any of the given hub slugs as a
+ -delimited component (e.g. "wikimedia-bpl+indiana" matches both "bpl" and
"indiana"), kills them, and posts a Slack notification.

Environment variables:
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY  — IAM credentials with ssm:SendCommand
  DPLA_SLACK_BOT_TOKEN                       — optional; skips Slack post if absent
"""

import argparse
import logging
import os
import shlex
import sys

import boto3
import requests

from ingest_wikimedia.partners import resolve_slug
from ingest_wikimedia.slack import post_message
from ingest_wikimedia.ssm import REGION, ssm_run


def _slack_fail(response_url: str, msg: str) -> None:
    """Print msg to stderr, post ephemeral reply to response_url if set, then exit 1."""
    print(msg, file=sys.stderr)
    if response_url:
        try:
            resp = requests.post(
                response_url,
                json={"response_type": "ephemeral", "text": msg},
                timeout=5,
            )
            resp.raise_for_status()
        except Exception as e:
            logging.warning("Failed to post to Slack response_url: %s", e)
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--partner", required=True)
    parser.add_argument("--response-url", default="")
    args = parser.parse_args()

    raw_url = args.response_url.strip()
    # Only accept genuine Slack response_url values — reject arbitrary POST targets.
    response_url = (
        raw_url if raw_url.startswith("https://hooks.slack.com/commands/") else ""
    )
    if raw_url and not response_url:
        print(f"Ignoring invalid response_url: {raw_url!r}", file=sys.stderr)

    try:
        target_tokens = shlex.split(args.partner)
    except ValueError as e:
        _slack_fail(response_url, f"Could not parse --partner: {e}")

    canonicals: list[str] = []
    for token in target_tokens:
        canonical = resolve_slug(token)
        if canonical is None:
            _slack_fail(response_url, f"Unknown hub: {token!r}")
        canonicals.append(canonical)

    if not canonicals:
        _slack_fail(response_url, "No hub slugs specified.")

    ssm = boto3.client("ssm", region_name=REGION)

    print("Listing tmux sessions...")
    try:
        tmux_list = ssm_run(ssm, "tmux ls 2>/dev/null || true")
    except Exception as e:
        _slack_fail(response_url, f"⚠️ Failed to list tmux sessions: {e}")

    canonical_set = set(canonicals)
    killed: list[str] = []
    for line in tmux_list.splitlines():
        session_name = line.split(":")[0].strip()
        if not session_name.startswith("wikimedia-"):
            continue
        session_hubs = set(session_name[len("wikimedia-") :].split("+"))
        if canonical_set & session_hubs:
            print(f"Killing session {session_name}...")
            try:
                ssm_run(ssm, f"tmux kill-session -t {shlex.quote(session_name)}")
                killed.append(session_name)
            except Exception as e:
                logging.warning("Failed to kill session %s: %s", session_name, e)

    slack_token = (os.environ.get("DPLA_SLACK_BOT_TOKEN") or "").strip()

    if killed:
        msg = f"🛑 Killed Wikimedia pipeline session(s): {', '.join(f'`{s}`' for s in killed)}"
    else:
        msg = f"No running Wikimedia sessions found for: {', '.join(f'`{c}`' for c in canonicals)}"

    print(msg)
    if slack_token:
        try:
            post_message(slack_token, msg)
        except Exception as e:
            logging.warning("Slack notification failed: %s", e)


if __name__ == "__main__":
    main()
