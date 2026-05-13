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

from ingest_wikimedia.partners import (
    canonical_matches_session_component,
    is_wikidata_id,
    resolve_slug,
    resolve_wikidata_id,
)
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

    # hub_canonicals: kill any session containing an institution from this hub
    # inst_labels: kill only the session with this specific institution label
    hub_canonicals: list[str] = []
    inst_labels: list[str] = []
    seen_canonicals: set[str] = set()
    seen_labels: set[str] = set()

    for token in target_tokens:
        if is_wikidata_id(token):
            resolved = resolve_wikidata_id(token)
            if not resolved:
                _slack_fail(
                    response_url,
                    f"No hub or institution found for Wikidata ID {token!r} in institutions_v2.json.",
                )
            for canonical, institution in resolved:
                if institution is not None:
                    label = institution.lower().replace(" ", "-")
                    if label not in seen_labels:
                        seen_labels.add(label)
                        inst_labels.append(label)
                else:
                    if canonical not in seen_canonicals:
                        seen_canonicals.add(canonical)
                        hub_canonicals.append(canonical)
        elif "|" in token:
            hub_part, institution = token.split("|", 1)
            canonical = resolve_slug(hub_part)
            if canonical is None:
                _slack_fail(response_url, f"Unknown hub: {hub_part!r}")
            label = institution.lower().replace(" ", "-")
            if label not in seen_labels:
                seen_labels.add(label)
                inst_labels.append(label)
        else:
            canonical = resolve_slug(token)
            if canonical is None:
                _slack_fail(response_url, f"Unknown hub: {token!r}")
            if canonical not in seen_canonicals:
                seen_canonicals.add(canonical)
                hub_canonicals.append(canonical)

    if not hub_canonicals and not inst_labels:
        _slack_fail(response_url, "No targets specified.")

    ssm = boto3.client("ssm", region_name=REGION)

    print("Listing tmux sessions...")
    try:
        tmux_list = ssm_run(ssm, "tmux ls 2>/dev/null || true")
    except Exception as e:
        _slack_fail(response_url, f"⚠️ Failed to list tmux sessions: {e}")

    label_set = set(inst_labels)
    canonical_set = set(hub_canonicals)
    killed: list[str] = []
    for line in tmux_list.splitlines():
        session_name = line.split(":")[0].strip()
        if not session_name.startswith("wikimedia-"):
            continue
        components = set(session_name[len("wikimedia-") :].split("+"))
        if components & label_set or any(
            canonical_matches_session_component(c, comp)
            for c in canonical_set
            for comp in components
        ):
            print(f"Killing session {session_name}...")
            try:
                ssm_run(ssm, f"tmux kill-session -t {shlex.quote(session_name)}")
                killed.append(session_name)
            except Exception as e:
                logging.warning("Failed to kill session %s: %s", session_name, e)

    slack_token = (os.environ.get("DPLA_SLACK_BOT_TOKEN") or "").strip()

    all_targets = [f"`{c}`" for c in hub_canonicals] + [f"`{lb}`" for lb in inst_labels]
    if killed:
        msg = f"🛑 Killed Wikimedia pipeline session(s): {', '.join(f'`{s}`' for s in killed)}"
    else:
        msg = f"No running Wikimedia sessions found for: {', '.join(all_targets)}"

    print(msg)
    if slack_token:
        try:
            post_message(slack_token, msg)
        except Exception as e:
            logging.warning("Slack notification failed: %s", e)


if __name__ == "__main__":
    main()
