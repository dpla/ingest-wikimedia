#!/usr/bin/env python3
"""Kill a running Wikimedia upload pipeline session on EC2.

Accepts the SAME target formats as wikimedia_launch.py so that any string
a user gave to ``/wikimedia-upload`` can be re-used verbatim with
``/wikimedia-upload kill``:

  * Hub slug              — ``bpl``
  * Hub|institution        — ``"indiana|Indiana State Library"``
  * Hub|institution|coll  — ``"nara|Center for Legislative Archives|RG46"``
  * Wikidata QID          — ``Q12345`` (resolves to hub or institution)
  * DPLA item ID          — 32-hex-char single-item launch label
  * Bare component slug   — ``indiana-state-library`` (kept for back-compat
                            with the prior matcher; matches any session
                            whose name contains it as a +-separated part)

Each token is normalized to the same session-label suffix that
wikimedia_launch.py would produce, then matched against running
``wikimedia-*`` tmux sessions and killed.

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
    is_dpla_id,
    is_wikidata_id,
    resolve_slug,
    resolve_wikidata_id,
    slugify_session_label_component,
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


class _UnknownHub(ValueError):
    """Raised by resolve_kill_components when a hub|institution token names an unknown hub."""


def resolve_kill_components(target_tokens: list[str]) -> list[frozenset[str]]:
    """Translate a list of user-supplied target tokens into session-label
    component GROUPS for kill-matching.

    Each token produces ONE group (a frozenset of session-label slugs).  A
    session is killed only if at least one group is a SUBSET of the
    session's component set (the part after ``wikimedia-``, split by ``+``).
    This scopes the kill to exactly the launch-time session label that
    produced it:

      * ``bpl`` → ``{bpl}`` — matches any wikimedia-bpl[+…] session
        (full-hub kill).
      * ``nara|Lyndon Baines Johnson Library`` →
        ``{nara, lyndon-baines-johnson-library}`` — matches
        wikimedia-nara+lyndon-baines-johnson-library[+…] and nothing
        else (NOT bare wikimedia-nara, NOT a different hub that happens
        to share an institution slug).
      * ``nara|Center for Legislative Archives|RG46`` →
        ``{nara, center-for-legislative-archives, rg-46}`` — matches
        ONLY the specific collection sub-session.  In particular, this
        is what fixes the original bug: a flat component list would
        kill the parent institution session (because
        ``center-for-legislative-archives`` alone intersected) AND any
        unrelated ``*+rg-46`` session.
      * Wikidata QID → one group per (canonical, institution_or_None)
        match, shaped the same way as the hub|institution form.
      * DPLA item ID → ``{first_8_hex}``; single-item launch labels
        carry that 8-hex string in the institution position.
      * Bare component slug (back-compat) → ``{slug}``, single-element
        group matching any session containing that component.

    Accepts the same token shapes as wikimedia_launch.  Groups are
    deduplicated in first-seen order.  Raises :class:`_UnknownHub` for
    ``hub|…`` tokens whose hub prefix doesn't resolve; raises
    :class:`ValueError` for unknown Wikidata QIDs — both so the caller
    can produce a user-facing Slack error.
    """
    groups: list[frozenset[str]] = []
    seen: set[frozenset[str]] = set()

    def _add(group: frozenset[str]) -> None:
        if group and group not in seen:
            seen.add(group)
            groups.append(group)

    def _slug_or_raise(name: str, role: str, token: str) -> str:
        """Slugify or raise with a user-facing error if the result is empty.

        slugify_session_label_component returns "" when the input is purely
        punctuation (e.g. "&&&", "..."), which would otherwise produce a
        group containing the empty string — impossible to match against any
        real tmux session label, so the kill would silently no-op rather
        than tell the user their input is malformed.
        """
        slug = slugify_session_label_component(name)
        if not slug:
            raise ValueError(
                f"{role} {name!r} in token {token!r} normalizes to an empty slug "
                f"(input is punctuation-only after slugifying); cannot match a session."
            )
        return slug

    for raw_token in target_tokens:
        # Strip once at the top so classification predicates (is_wikidata_id,
        # is_dpla_id) and slug lookups all see the same normalized form. Without
        # this, a token like " 405714D9... " or " indiana-state-library " would
        # silently fall through to the bare-slug branch (or fail entirely),
        # never matching anything in main()'s subset check.
        token = raw_token.strip()
        if not token:
            continue
        if is_wikidata_id(token):
            resolved = resolve_wikidata_id(token)
            if not resolved:
                raise ValueError(
                    f"No hub or institution found for Wikidata ID {token!r} in institutions_v2.json."
                )
            for canonical, institution in resolved:
                if institution:
                    _add(
                        frozenset(
                            {
                                canonical,
                                _slug_or_raise(institution, "Institution", token),
                            }
                        )
                    )
                else:
                    _add(frozenset({canonical}))
        elif is_dpla_id(token):
            # Single-item launch labels carry the first 8 hex chars of the
            # DPLA ID as their institution-position slug (see
            # wikimedia_launch._add_target); match that.  An 8-hex string
            # is unique enough that a single-component group is safe.
            _add(frozenset({token.lower()[:8]}))
        elif "|" in token:
            # hub | institution [| collection]: build a group containing
            # the canonical hub plus the institution/collection slugs.
            # main()'s subset check then requires ALL of them be present
            # together — so a collection token can't kill the parent
            # institution session, and an institution slug can't cross
            # to a different hub that happens to share it.
            parts = token.split("|", 2)
            hub_part = parts[0].strip()
            canonical = resolve_slug(hub_part)
            if canonical is None:
                raise _UnknownHub(f"Unknown hub {hub_part!r} in target {token!r}.")
            group: set[str] = {canonical}
            if len(parts) >= 2:
                inst = parts[1].strip()
                if inst:
                    group.add(_slug_or_raise(inst, "Institution", token))
            if len(parts) == 3:
                coll = parts[2].strip()
                if coll:
                    group.add(_slug_or_raise(coll, "Collection", token))
            _add(frozenset(group))
        else:
            # Bare hub slug or already-a-component slug — single-component
            # group matching any session containing that component.
            _add(frozenset({resolve_slug(token) or token}))
    return groups


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

    try:
        kill_groups = resolve_kill_components(target_tokens)
    except (ValueError, _UnknownHub) as e:
        _slack_fail(response_url, str(e))

    if not kill_groups:
        _slack_fail(response_url, "No targets specified.")

    ssm = boto3.client("ssm", region_name=REGION)

    print("Listing tmux sessions...")
    try:
        tmux_list = ssm_run(ssm, "tmux ls 2>/dev/null || true")
    except Exception as e:
        _slack_fail(response_url, f"⚠️ Failed to list tmux sessions: {e}")

    killed: list[str] = []
    failed: list[str] = []
    for line in tmux_list.splitlines():
        session_name = line.split(":")[0].strip()
        if not session_name.startswith("wikimedia-"):
            continue
        # Subset, not intersection: every component in at least one group
        # must be present in the session's label.  Intersection would let
        # a collection token kill the parent institution session, and an
        # institution slug bleed across hubs — see resolve_kill_components.
        components = set(session_name[len("wikimedia-") :].split("+"))
        if any(group <= components for group in kill_groups):
            print(f"Killing session {session_name}...")
            try:
                ssm_run(ssm, f"tmux kill-session -t {shlex.quote(session_name)}")
                killed.append(session_name)
            except Exception as e:
                logging.warning("Failed to kill session %s: %s", session_name, e)
                failed.append(session_name)

    slack_token = (os.environ.get("DPLA_SLACK_BOT_TOKEN") or "").strip()

    if failed:
        msg = (
            f"⚠️ Failed to kill Wikimedia pipeline session(s): "
            f"{', '.join(f'`{s}`' for s in failed)}"
        )
        if killed:
            msg += f"\n🛑 Also killed: {', '.join(f'`{s}`' for s in killed)}"
    elif killed:
        msg = f"🛑 Killed Wikimedia pipeline session(s): {', '.join(f'`{s}`' for s in killed)}"
    else:
        # Render each group as the '+'-joined component list a matching
        # tmux session would carry; that's the most readable form for the
        # user and mirrors how the session names actually look.
        group_strs = ["+".join(sorted(g)) for g in kill_groups]
        msg = f"No running Wikimedia sessions found matching: {', '.join(f'`{s}`' for s in group_strs)}"

    print(msg)
    if slack_token:
        try:
            post_message(slack_token, msg)
        except Exception as e:
            logging.warning("Slack notification failed: %s", e)


if __name__ == "__main__":
    main()
