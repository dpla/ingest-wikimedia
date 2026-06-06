#!/usr/bin/env python3
"""Launch a Wikimedia upload pipeline session on EC2 for one or more partner hubs.

Runs as a GitHub Actions workflow step triggered by workflow_dispatch or the
/wikimedia-upload Slack slash command via Lambda. Updates EC2 code, checks for
conflicting sessions, launches the full pipeline in a single tmux session (with
all targets run sequentially), and posts a Slack confirmation to #tech-alerts.

Each target in --partner is a hub slug ("bpl"), a hub|institution pair
("indiana|Indiana State Library"), a hub|institution|collection triple
("bpl|Digital Commonwealth|Boston City Archives"), a Wikidata QID ("Q1234567"),
or a 32-hex-char DPLA item ID ("abc123def456789012345678901234ab").  Multiple
targets run sequentially in one tmux session; a failing target posts a Slack
error and continues with the next target.

For DPLA item ID targets the ID generation phase is skipped — the ID is written
directly to the CSV.  Eligibility (rights statement + media presence) is verified
on EC2 before launch.  Per-phase Slack notifications are suppressed; only one
launch message and one completion message are posted.

Environment variables:
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY  — IAM credentials with ssm:SendCommand
  DPLA_SLACK_BOT_TOKEN                       — optional; skips Slack post if absent
"""

import argparse
import logging
import os
import shlex
import sys
from typing import NoReturn

import boto3
import requests

from ingest_wikimedia.partners import (
    PARTNER_DIR,
    is_dpla_id,
    is_upload_eligible,
    is_wikidata_id,
    parse_session_labels,
    resolve_slug,
    resolve_wikidata_id,
    slugify_session_label_component,
)
from ingest_wikimedia.slack import post_message
from ingest_wikimedia.ssm import REGION, ssm_run, stage_and_launch_tmux

# Each ingest session peaks at ~300–500 MB; 30% of 7.6 GB leaves headroom for 4–5 concurrent sessions.
MEMORY_HEADROOM_PCT = 30

# Tmux-safe session-label slugifier (lowercase alphanumeric + hyphens) lives in
# ingest_wikimedia.partners as slugify_session_label_component so that
# wikimedia_kill.py uses the IDENTICAL function — otherwise institutions whose
# names contain stripped characters (e.g. ``AT&T``) would launch under one
# slug and never match on kill.  Keep a short local alias for readability of
# the call sites below.
_slugify = slugify_session_label_component


def _parse_bool(value: str) -> bool:
    """Parse a GitHub Actions boolean-string input into a real bool.

    GH Actions passes ``workflow_dispatch`` boolean inputs as the literal
    strings ``"true"`` / ``"false"`` (case-insensitive). Anything else
    (including empty, ``"yes"``, ``"1"``) is treated as falsy. Used for
    ``--force``, ``--refresh-only``, and ``--sdc-only`` so the polarity
    stays consistent across all three flags.
    """
    return value.lower() == "true"


def _slack_fail(response_url: str, msg: str) -> NoReturn:
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
    parser.add_argument("--force", default="false")
    parser.add_argument("--response-url", default="")
    parser.add_argument("--max-age-days", default="")
    parser.add_argument("--refresh-only", default="false")
    parser.add_argument("--sdc-only", default="false")
    args = parser.parse_args()

    force = _parse_bool(args.force)
    refresh_only = _parse_bool(args.refresh_only)
    sdc_only = _parse_bool(args.sdc_only)

    # Normalize the response_url first so the mutual-exclusion check below
    # can use the validated value rather than re-implementing the same
    # `startswith("https://hooks.slack.com/commands/")` guard inline.
    raw_url = args.response_url.strip()
    # Only accept genuine Slack response_url values — reject arbitrary POST targets.
    response_url = (
        raw_url if raw_url.startswith("https://hooks.slack.com/commands/") else ""
    )
    if raw_url and not response_url:
        print(f"Ignoring invalid response_url: {raw_url!r}", file=sys.stderr)

    if refresh_only and sdc_only:
        _slack_fail(
            response_url,
            "Cannot combine --refresh-only and --sdc-only — they're mutually"
            " exclusive run modes (refresh skips upload + SDC; sdc-only skips"
            " download + upload).",
        )
    max_age_days: int | None = None
    if args.max_age_days.strip():
        try:
            max_age_days = int(args.max_age_days)
            if max_age_days <= 0:
                raise ValueError
        except ValueError:
            _slack_fail(
                response_url,
                f"Invalid --max-age-days value: {args.max_age_days!r} (must be a positive integer).",
            )

    # --partner may be a shlex-encoded list: 'bpl "indiana|Indiana State Library"'
    try:
        target_tokens = shlex.split(args.partner)
    except ValueError as e:
        _slack_fail(response_url, f"Could not parse --partner: {e}")

    # Validate each target and build
    # (canonical, institutions_tuple, label, dpla_id_or_None, collection_or_None) tuples.
    # ``institutions_tuple`` is ``()`` for hub-level, ``(name,)`` for single-
    # institution, or ``(n1, n2, …)`` for a combined session covering multiple
    # institutions under one hub (used when a single Wikidata QID resolves to
    # multiple institutions in the same hub).
    # Dedup by full target string so the same hub may appear with different institutions
    # (e.g. two QIDs that both resolve into the same hub but different institutions).
    seen_target_strs: set[str] = set()
    seen_canonicals: dict[str, None] = {}  # insertion-ordered; for conflict detection
    seen_session_labels: dict[
        str, None
    ] = {}  # insertion-ordered; drives session naming
    targets: list[tuple[str, tuple[str, ...], str, str | None, str | None]] = []
    # DPLA item ID tokens collected separately; resolved via EC2 before target building.
    dpla_id_tokens: list[str] = []
    # Per-target validation warnings; populated by _add_target and the conflict
    # check. If some targets are skipped but others remain valid, a summary is
    # posted to the caller before launch rather than aborting the whole run.
    skipped_warnings: list[str] = []

    def _add_target(
        canonical: str,
        institutions: tuple[str, ...] = (),
        dpla_id: str | None = None,
        collection: str | None = None,
    ) -> None:
        # ``institutions`` semantics:
        #   ``()``         → hub-level (no institution filter at get-ids-es time)
        #   ``(name,)``    → single institution (the historical / pipe-target case)
        #   ``(n1, n2, …)`` → combined session covering multiple institutions
        #                     under the same hub. Used when a single Wikidata
        #                     QID resolves to N institutions in one hub; the
        #                     pipeline runs ONE ``get-ids-es`` (with N
        #                     ``--institution`` flags), one downloader, one
        #                     uploader, one sdc-sync — so the operator sees
        #                     one tmux session and one set of Slack
        #                     notifications instead of N chained sessions.
        if collection is not None and len(institutions) != 1:
            skipped_warnings.append(
                f"Collection '{collection}' requires exactly one institution"
                " (collection scoping is per-institution)."
            )
            return
        stripped: list[str] = []
        for name in institutions:
            name = name.strip()
            if not name:
                skipped_warnings.append(f"'{canonical}|': empty institution name.")
                return
            stripped.append(name)
        institutions = tuple(stripped)
        if dpla_id is None and canonical != "nara" and not institutions:
            # Hub-level target: check that any institution in the hub is eligible.
            # Skipped for DPLA item ID targets — item-level eligibility (rights
            # statement + media presence) was already verified by resolve-dpla-ids.
            # Institution-level and collection-level eligibility is not checked here —
            # get-ids-es enforces it at runtime, and the per-target failure handler
            # (notify_pipeline_fail) catches any rejection and continues with remaining
            # targets. NARA hub-level runs use get-ids-nara which filters itself.
            try:
                eligible = is_upload_eligible(canonical)
            except Exception as e:
                skipped_warnings.append(
                    f"'{canonical}': failed to check upload eligibility: {e}"
                )
                return
            if not eligible:
                skipped_warnings.append(
                    f"'{canonical}': not upload-eligible per institutions_v2.json."
                )
                return
        if collection is not None:
            target_str = f"{canonical}|{institutions[0]}|{collection}"
        elif len(institutions) == 1:
            target_str = f"{canonical}|{institutions[0]}"
        elif institutions:
            # Multi-institution: dedup key joins all names so two different
            # institution sets under the same hub produce two different
            # sessions, but the same QID resolved twice in one command
            # produces a "duplicate target" warning rather than two
            # near-identical sessions.
            target_str = f"{canonical}|" + "+".join(institutions)
        elif dpla_id is not None:
            target_str = dpla_id
        else:
            target_str = canonical
        if target_str in seen_target_strs:
            skipped_warnings.append(f"'{target_str}': duplicate target.")
            return
        seen_target_strs.add(target_str)
        seen_canonicals[canonical] = None
        if dpla_id is not None:
            # Use the first 8 hex chars of the ID as the label's institution component.
            # This is long enough to distinguish concurrent single-item sessions for
            # the same hub while keeping session names readable.
            inst_label: str | None = dpla_id[:8]
        elif institutions:
            first_slug = _slugify(institutions[0])
            if not first_slug:
                skipped_warnings.append(
                    f"'{canonical}|{institutions[0]}': institution name normalizes to an empty slug."
                )
                return
            if len(institutions) == 1:
                inst_label = first_slug
            else:
                # Multi-institution session label: keep it short and parseable.
                # ``-and-N-more`` is human-readable in tmux ls output and in
                # Slack notifications, and the leading slug pins which hub +
                # at-least-one-institution this session is for.
                inst_label = f"{first_slug}-and-{len(institutions) - 1}-more"
        else:
            inst_label = None
        if collection is not None:
            coll_label = _slugify(collection)
            if not coll_label:
                skipped_warnings.append(
                    f"'{target_str}': collection name normalizes to an empty slug."
                )
                return
            label = f"{canonical}+{inst_label}+{coll_label}"
        else:
            label = f"{canonical}+{inst_label}" if inst_label is not None else canonical
        if label in seen_session_labels:
            skipped_warnings.append(
                f"'{target_str}': normalizes to the same session label ('{label}') as a previous target."
            )
            return
        seen_session_labels[label] = None
        targets.append((canonical, institutions, label, dpla_id, collection))

    for token in target_tokens:
        if is_wikidata_id(token):
            resolved = resolve_wikidata_id(token)
            if not resolved:
                skipped_warnings.append(
                    f"Wikidata ID {token!r}: not found in institutions_v2.json."
                )
                continue
            # Group resolved pairs by canonical hub so multiple institutions
            # under the same hub collapse into a single combined session
            # (one tmux session, one get-ids-es with N --institution flags,
            # one downloader / uploader / sdc-sync run, one set of Slack
            # notifications). Cross-hub QIDs — rare; same QID present under
            # institutions in different hubs — still produce one session per
            # hub, but each of those sessions is itself combined across all
            # institutions matching the QID in that hub.
            by_hub: dict[str, list[str | None]] = {}
            for canonical, institution in resolved:
                by_hub.setdefault(canonical, []).append(institution)
            for canonical, inst_list in by_hub.items():
                if None in inst_list:
                    # The QID matched the hub itself — that's strictly broader
                    # than any per-institution match, so the hub-level scope
                    # wins (no --institution filter, all eligible institutions
                    # processed).
                    _add_target(canonical, institutions=())
                else:
                    # All matches are institution-level; combine them.
                    # ``inst_list`` only contains non-None strings at this
                    # branch, so the cast is safe.
                    _add_target(
                        canonical,
                        institutions=tuple(
                            name for name in inst_list if name is not None
                        ),
                    )
        elif is_dpla_id(token):
            # Collect for batch resolution via EC2 after local parsing is done.
            # Normalise to lowercase — ES and S3 paths expect the canonical form.
            dpla_id_tokens.append(token.strip().lower())
        elif "|" in token:
            token_parts = token.split("|", 2)
            hub_part = token_parts[0]
            canonical = resolve_slug(hub_part)
            if canonical is None:
                skipped_warnings.append(f"'{token}': unknown hub '{hub_part}'.")
                continue
            if len(token_parts) == 3:
                _, institution, collection = token_parts
                _add_target(
                    canonical, institutions=(institution,), collection=collection
                )
            else:
                _, institution = token_parts
                _add_target(canonical, institutions=(institution,))
        else:
            canonical = resolve_slug(token)
            if canonical is None:
                skipped_warnings.append(f"'{token}': unknown hub slug.")
                continue
            _add_target(canonical, institutions=())

    slack_token = (os.environ.get("DPLA_SLACK_BOT_TOKEN") or "").strip()
    ssm = boto3.client("ssm", region_name=REGION)

    # Update EC2 code first so that resolve-dpla-ids and the pipeline both run
    # the latest version from this branch, not whatever was previously deployed.
    # Pin to GITHUB_SHA when available (always set in GitHub Actions) so that
    # feature-branch dispatches deploy the branch code, not the default branch.
    print("Updating EC2 code...")
    github_sha = (os.environ.get("GITHUB_SHA") or "").strip()
    pin_step = (
        f"cd /tmp/ingest-wikimedia-update && "
        f"git fetch --depth 1 origin {shlex.quote(github_sha)} && "
        f"git checkout --detach {shlex.quote(github_sha)} && "
        "cd /tmp && "
        if github_sha
        else ""
    )
    # `ssm_run` wraps every command in `sudo -u ec2-user bash -c` by
    # default. That's correct for the actual update (we want git clone, cp,
    # and uv sync to run as ec2-user so they never create root-owned
    # files), but `chown -R` of root-owned files needs CAP_CHOWN, which
    # ec2-user does not have. So the heal step uses ssm_run(..., as_root=True)
    # to bypass the wrapper and run with the AWS-RunShellScript document's
    # default root context.
    #
    # The earlier "Permission denied: ... lxml/__pycache__" failure mode
    # came from prior root-context Python imports (back when the update
    # itself ran as root) leaving root-owned `__pycache__/` inside an
    # otherwise ec2-user-owned venv. The heal step cleans those up; the
    # ec2-user-run update step prevents them from being recreated.
    heal_cmd = (
        "chown -R ec2-user:ec2-user /home/ec2-user/ingest-wikimedia/ && "
        "(chown -R ec2-user:ec2-user /tmp/ingest-wikimedia-update 2>/dev/null || true)"
    )
    try:
        ssm_run(ssm, heal_cmd, as_root=True)
    except Exception as e:
        _slack_fail(response_url, f"⚠️ Failed to heal EC2 file ownership: {e}")

    update_cmd = (
        "cd /tmp && rm -rf ingest-wikimedia-update && "
        "git clone --depth 1 https://github.com/dpla/ingest-wikimedia.git ingest-wikimedia-update && "
        + pin_step
        + "cp -r ingest-wikimedia-update/ingest_wikimedia/* /home/ec2-user/ingest-wikimedia/ingest_wikimedia/ && "
        "cp -r ingest-wikimedia-update/tools/* /home/ec2-user/ingest-wikimedia/tools/ && "
        "cp ingest-wikimedia-update/pyproject.toml /home/ec2-user/ingest-wikimedia/pyproject.toml && "
        "cp ingest-wikimedia-update/uv.lock /home/ec2-user/ingest-wikimedia/uv.lock && "
        "/home/ec2-user/.local/bin/uv sync --project /home/ec2-user/ingest-wikimedia && echo UPDATE_DONE"
    )
    out = ""
    try:
        out = ssm_run(ssm, update_cmd)
    except Exception as e:
        _slack_fail(response_url, f"⚠️ Failed to update EC2 code: {e}")
    if "UPDATE_DONE" not in out:
        _slack_fail(
            response_url,
            "⚠️ EC2 code update did not confirm completion. Check the GitHub Actions run for details.",
        )
    print("EC2 code updated.")

    # Resolve any DPLA item ID tokens via EC2. resolve-dpla-ids checks the ES index
    # for rights eligibility and media presence, then stages the item metadata to S3
    # (same as get-ids-es) so the downloader and uploader can proceed without changes.
    if dpla_id_tokens:
        print(f"Resolving {len(dpla_id_tokens)} DPLA ID(s)...")
        resolve_cmd = (
            "/home/ec2-user/ingest-wikimedia/.venv/bin/resolve-dpla-ids "
            + " ".join(shlex.quote(i) for i in dpla_id_tokens)
        )
        resolve_out = None
        try:
            resolve_out = ssm_run(ssm, resolve_cmd)
        except Exception as e:
            for item_id in dpla_id_tokens:
                skipped_warnings.append(
                    f"DPLA ID `{item_id}`: could not resolve ({e})."
                )
        if resolve_out is not None:
            unresolved_ids = set(dpla_id_tokens)
            for line in resolve_out.splitlines():
                parts = line.strip().split(" ", 1)
                if len(parts) < 2:
                    skipped_warnings.append(f"Unexpected resolver output: {line!r}.")
                    continue
                item_id, status = parts
                unresolved_ids.discard(item_id)
                if status == "NOT_FOUND":
                    skipped_warnings.append(f"DPLA ID `{item_id}`: not found in index.")
                elif status.startswith("INELIGIBLE:"):
                    reason = status.removeprefix("INELIGIBLE:")
                    skipped_warnings.append(
                        f"DPLA ID `{item_id}`: not eligible ({reason})."
                    )
                elif status.startswith("HUB="):
                    _add_target(status.removeprefix("HUB="), None, dpla_id=item_id)
                elif status.startswith("ERROR:"):
                    skipped_warnings.append(
                        f"DPLA ID `{item_id}`: resolve error — {status.removeprefix('ERROR:')}."
                    )
                else:
                    skipped_warnings.append(
                        f"DPLA ID `{item_id}`: unexpected resolver status `{status}`."
                    )
            for item_id in sorted(unresolved_ids):
                skipped_warnings.append(
                    f"DPLA ID `{item_id}`: resolver returned no status."
                )

    if not targets:
        detail = ("\n• " + "\n• ".join(skipped_warnings)) if skipped_warnings else ""
        _slack_fail(response_url, f"No valid targets to launch.{detail}")

    # `--sdc-only` is meaningful only for targets whose ID-generation step
    # re-stages sdc.json. Two target types use a different path:
    #   * Single-item DPLA IDs — the launcher writes the one ID via `printf`
    #     to skip the enumeration phase; `resolve-dpla-ids` (run at startup)
    #     stages `dpla-map.json` but NOT `sdc.json`.
    #   * NARA hub-level — uses `get-ids-nara` (NARA catalog enumeration,
    #     not ingestion3 ES), which also doesn't write `sdc.json`.
    # For these, sdc-sync will replay whatever sidecar the *original* upload
    # run wrote. That's fine for re-running PR-#251-style code changes
    # against a known item, but operators backfilling for upstream mapping
    # changes need to know they won't pick up. Warn loudly rather than
    # silently using stale data.
    if sdc_only:
        stale_sdc_target_labels = [
            lbl
            for canonical, institutions, lbl, dpla_id, _ in targets
            if dpla_id is not None or (canonical == "nara" and not institutions)
        ]
        if stale_sdc_target_labels:
            print(
                "Warning: --sdc-only with single-item DPLA IDs or NARA"
                " hub-level targets will use the existing sdc.json sidecars"
                " (last written by get-ids-es during the original upload"
                " run). These targets cannot re-stage sdc.json. Affected:"
                f" {', '.join(stale_sdc_target_labels)}.",
                file=sys.stderr,
            )
        if max_age_days is not None:
            print(
                "Warning: --max-age-days is ignored in --sdc-only mode"
                " (no download phase runs).",
                file=sys.stderr,
            )

    # Session name uses + as separator (unambiguous since slugs/institution names use -).
    # Institution-level targets include the hub slug as a prefix so the status script
    # can derive the EC2 directory: "indiana|Indiana State Library" → wikimedia-indiana+indiana-state-library.
    session_name = "wikimedia-" + "+".join(lbl for _, _, lbl, _, _ in targets)

    print("Checking instance memory...")
    try:
        mem_out = ssm_run(ssm, "free -m | awk 'NR==2{print $2, $7}'")
    except Exception as e:
        _slack_fail(response_url, f"⚠️ Failed to check instance memory: {e}")
    parts = mem_out.split()
    if len(parts) != 2:
        _slack_fail(response_url, f"⚠️ Unexpected memory output: {mem_out!r}")
    try:
        total_mb, available_mb = int(parts[0]), int(parts[1])
        pct_available = available_mb * 100 // total_mb
    except (ValueError, ZeroDivisionError) as e:
        _slack_fail(response_url, f"⚠️ Could not parse memory output ({mem_out!r}): {e}")
    print(f"Memory: {pct_available}% available ({available_mb} MB of {total_mb} MB).")
    if pct_available < MEMORY_HEADROOM_PCT:
        _slack_fail(
            response_url,
            f"⚠️ Cannot launch `{session_name}`: only {pct_available}% memory available"
            f" ({available_mb} MB of {total_mb} MB). Threshold is {MEMORY_HEADROOM_PCT}%.",
        )

    # Verify every requested partner has a working directory on EC2. Pywikibot
    # expects per-partner config.toml / user-config.py / user-password.py /
    # apicache/; without them the pipeline's first `cd {base}` silently fails.
    # Any missing partner dir is auto-initialized by copying the required
    # pywikibot config from a template partner (bpl) — the bot account and
    # config are the same across all partners, so this is a safe one-shot
    # bootstrap. apicache/ is populated by pywikibot at first login.
    unique_pdirs = sorted({PARTNER_DIR.get(c, c) for c, _, _, _, _ in targets})
    print(f"Checking partner directories exist: {unique_pdirs}")
    check_cmd = "; ".join(
        f"test -d /home/ec2-user/ingest-wikimedia/{shlex.quote(d)} || echo MISSING:{d}"
        for d in unique_pdirs
    )
    try:
        dir_check_out = ssm_run(ssm, check_cmd)
    except Exception as e:
        _slack_fail(response_url, f"⚠️ Failed to check partner directories: {e}")
    missing_dirs = sorted(
        line.split(":", 1)[1]
        for line in dir_check_out.splitlines()
        if line.startswith("MISSING:")
    )
    if missing_dirs:
        # The bpl directory is the bootstrap template — it's a primary partner
        # that always exists in practice. Refuse to bootstrap if even bpl is
        # missing; that's a much bigger setup problem worth surfacing loudly.
        if "bpl" in missing_dirs:
            _slack_fail(
                response_url,
                f"⚠️ Cannot launch `{session_name}`: template partner directory"
                f" `bpl` is missing on EC2. The bot relies on it as the source"
                f" of pywikibot configuration when bootstrapping new partner"
                f" directories. Manual setup required.",
            )
        print(f"Bootstrapping missing partner directories: {missing_dirs}")
        # Single SSM round-trip: mkdir + cp -np for each missing dir. -n
        # ("no-clobber") makes the copy idempotent if a config file already
        # happens to be in place.
        init_cmd = " && ".join(
            f"mkdir -p /home/ec2-user/ingest-wikimedia/{shlex.quote(d)} && "
            f"cp -np /home/ec2-user/ingest-wikimedia/bpl/config.toml"
            f" /home/ec2-user/ingest-wikimedia/bpl/user-config.py"
            f" /home/ec2-user/ingest-wikimedia/bpl/user-password.py"
            f" /home/ec2-user/ingest-wikimedia/{shlex.quote(d)}/"
            for d in missing_dirs
        )
        try:
            ssm_run(ssm, init_cmd)
        except Exception as e:
            _slack_fail(
                response_url,
                f"⚠️ Cannot launch `{session_name}`: failed to bootstrap"
                f" partner directories {missing_dirs}: {e}",
            )
        bootstrapped_list = ", ".join(f"`{d}`" for d in missing_dirs)
        print(f"Auto-bootstrapped partner directories: {bootstrapped_list}")
        # Surface the bootstrap to Slack so the user sees what was created.
        if slack_token:
            try:
                post_message(
                    slack_token,
                    f"🛠️ Bootstrapped new partner directories on EC2 for"
                    f" `{session_name}`: {bootstrapped_list} (copied pywikibot"
                    f" config from `bpl`).",
                )
            except Exception as e:
                print(f"Failed to post bootstrap notification: {e}")

    # Check for any existing session that includes one of the requested targets.
    # Maps each requested label to the existing session name(s) it conflicts with.
    print(f"Checking for existing sessions that overlap with {session_name}...")
    try:
        tmux_list = ssm_run(ssm, "tmux ls 2>/dev/null || true")
    except Exception as e:
        _slack_fail(response_url, f"⚠️ Failed to list tmux sessions: {e}")
    label_conflicts: dict[str, list[str]] = {}
    for line in tmux_list.splitlines():
        existing_name = line.split(":")[0].strip()
        if not existing_name.startswith("wikimedia-"):
            continue
        existing_labels = set(parse_session_labels(existing_name[len("wikimedia-") :]))
        for canonical, institutions, label, dpla_id, collection in targets:
            if not institutions and dpla_id is None:
                # Hub-level request conflicts with any existing session touching this hub
                # (hub-level, institution-level, or collection-level for the same hub).
                conflicts_this = any(
                    lbl == canonical or lbl.startswith(f"{canonical}+")
                    for lbl in existing_labels
                )
            elif collection is not None:
                # Collection-level request conflicts with:
                #   1. An existing hub-level session for the same hub
                #   2. An existing institution-level session for the same institution
                #      (the collection is a subset; running both simultaneously would
                #      duplicate work, though it is technically idempotent)
                #   3. An existing session for the exact same collection label
                # A collection does NOT conflict with a different collection from the
                # same institution — those are independent, disjoint subsets.
                # ``institutions`` is exactly one element when collection is set
                # (enforced by _add_target).
                inst_label = f"{canonical}+{_slugify(institutions[0])}"
                conflicts_this = (
                    canonical in existing_labels
                    or inst_label in existing_labels
                    or label in existing_labels
                )
            elif dpla_id is None:
                # Institution-level (or multi-institution-combined) requests
                # conflict with:
                #   1. An existing hub-level session for the same hub
                #   2. An existing session for the exact same label
                #   3. Any existing collection-level session for the same label
                #      (the collection is a strict subset; the institution run would
                #      duplicate work on those items, symmetric with collection→institution)
                # Two institution-level sessions for different institution sets
                # within the same hub do NOT conflict — combined and single sets
                # are treated symmetrically.
                conflicts_this = (
                    canonical in existing_labels
                    or label in existing_labels
                    or any(lbl.startswith(f"{label}+") for lbl in existing_labels)
                )
            else:
                # Single-item requests conflict only with a hub-wide session for the
                # same hub or an exact duplicate single-item label.
                conflicts_this = (
                    canonical in existing_labels or label in existing_labels
                )
            if conflicts_this:
                label_conflicts.setdefault(label, []).append(existing_name)
    if label_conflicts:
        if force:
            sessions_to_kill = {
                name for names in label_conflicts.values() for name in names
            }
            for existing_name in sessions_to_kill:
                print(f"Existing session found: {existing_name}; killing it (--force).")
                try:
                    ssm_run(ssm, f"tmux kill-session -t {shlex.quote(existing_name)}")
                except Exception as e:
                    _slack_fail(
                        response_url, f"⚠️ Failed to kill session `{existing_name}`: {e}"
                    )
        else:
            for label, existing_names in label_conflicts.items():
                existing_str = ", ".join(f"`{n}`" for n in existing_names)
                skipped_warnings.append(
                    f"`{label}`: conflicts with already-running session(s) {existing_str}."
                    " Use force=true to restart."
                )
            conflicting_labels = set(label_conflicts)
            targets[:] = [
                (c, i, lbl, did, col)
                for c, i, lbl, did, col in targets
                if lbl not in conflicting_labels
            ]
            for lbl in conflicting_labels:
                seen_session_labels.pop(lbl, None)
            if not targets:
                detail = "\n• " + "\n• ".join(skipped_warnings)
                _slack_fail(
                    response_url,
                    f"No valid targets remain — all conflict with running sessions.{detail}",
                )

    if skipped_warnings:
        warning_text = "⚠️ Skipped targets:\n• " + "\n• ".join(skipped_warnings)
        print(warning_text, file=sys.stderr)
        if response_url:
            try:
                requests.post(
                    response_url,
                    json={"response_type": "ephemeral", "text": warning_text},
                    timeout=5,
                ).raise_for_status()
            except Exception as e:
                logging.warning("Failed to post skip warnings to Slack: %s", e)

    # Recompute after conflict filtering — derive from targets to guarantee
    # the label order in the session name matches the surviving target list.
    session_name = "wikimedia-" + "+".join(lbl for _, _, lbl, _, _ in targets)

    # Build the pipeline command for all targets.
    # Setup (sourcing) runs once and gates all targets via &&.
    # Each target block runs its steps chained with &&. For hub targets these are
    # get-ids, downloader, and uploader. For DPLA item ID targets, the ID is written
    # directly to the CSV (metadata was staged by resolve-dpla-ids), so only
    # downloader and uploader run.
    # A failing block posts a Slack error notification then continues to the next
    # target (|| handler + ; separator).
    # The cd is required because config.toml is read from CWD.
    #
    # notify_fail_cmd captures the failing step's exit code via `rc=$?` so the
    # Slack message can decode signals like 137 (SIGKILL / probable OOM) and
    # 143 (SIGTERM). No backslash escaping needed on `$?` / `$rc`: pipeline_cmd
    # is staged to a script file via stage_and_launch_tmux below and read
    # directly by bash, so the inner shell sees raw `$?` / `$rc` and expands
    # them at runtime against the actual failing step.
    notify_fail_cmd = (
        "rc=$?; WIKIMEDIA_LAST_EXIT=$rc python3 -c "
        "'from ingest_wikimedia.slack import notify_pipeline_fail; notify_pipeline_fail()'"
    )
    setup = " && ".join(
        [
            "source ~/.bashrc",
            "source /home/ec2-user/ingest-wikimedia/.venv/bin/activate",
        ]
    )
    target_blocks = []
    last_idx = len(targets) - 1
    for idx, (canonical, institutions, session_label, dpla_id, collection) in enumerate(
        targets
    ):
        pdir = PARTNER_DIR.get(canonical, canonical)
        base = f"/home/ec2-user/ingest-wikimedia/{pdir}"
        # Use a per-target CSV filename so concurrent institution-level and
        # collection-level sessions don't clobber each other's ID lists.
        csv_file = f"{session_label}.csv"
        if dpla_id is not None:
            # Single-item target: metadata was already staged to S3 by resolve-dpla-ids.
            # Write the one ID directly to the CSV; no ID-generation phase needed.
            get_ids_cmd = f"printf '%s\\n' {shlex.quote(dpla_id)} > {csv_file}"
        elif canonical == "nara" and not institutions:
            get_ids_cmd = f"get-ids-nara > {csv_file}"
        else:
            get_ids_cmd = f"get-ids-es {canonical}"
            # ``--institution`` is repeated per name when the QID resolved
            # to multiple institutions under this hub; get-ids-es ORs them
            # in the Elasticsearch dataProvider filter.
            for inst in institutions:
                get_ids_cmd += f" --institution {shlex.quote(inst)}"
            if collection is not None:
                get_ids_cmd += f" --collection {shlex.quote(collection)}"
            get_ids_cmd += f" > {csv_file}"
        # Export the session label (and single-item flag) before the group so the
        # failure handler and logging have the correct context even if cd itself fails.
        # WIKIMEDIA_SINGLE_ITEM must be explicitly unset for hub targets so that it
        # does not bleed across blocks when targets are mixed in the same session.
        single_item_env = (
            "export WIKIMEDIA_SINGLE_ITEM=1"
            if dpla_id is not None
            else "unset WIKIMEDIA_SINGLE_ITEM"
        )
        # WIKIMEDIA_PARTNER_DIR is read by notify_pipeline_fail() to locate
        # the most recent log for this target and include a tail + counts in
        # the Slack failure message.  WIKIMEDIA_TARGET_IS_LAST switches the
        # failure suffix language ("no further targets in batch" vs
        # "skipping to next target"); the unset is required so a failure
        # earlier in the batch doesn't inherit a stale "is last" flag.
        is_last_env = (
            "export WIKIMEDIA_TARGET_IS_LAST=1"
            if idx == last_idx
            else "unset WIKIMEDIA_TARGET_IS_LAST"
        )
        label_export = (
            f"export WIKIMEDIA_SESSION_LABEL={shlex.quote(session_label)}; "
            f"export WIKIMEDIA_PARTNER_DIR={shlex.quote(base)}; "
            f"{is_last_env}; "
            f"{single_item_env}"
        )
        if sdc_only:
            # SDC-only backfill: re-enumerate the partner's items (which
            # also refreshes sdc.json sidecars from the latest ingestion3
            # data) then run sdc-sync. No downloader, no uploader — the
            # caller is reconciling SDC for items the uploader already
            # processed in a prior session.
            pipeline_steps = [
                f"cd {base}",
                get_ids_cmd,
                f"sdc-sync --partner {canonical} --ids-file {csv_file}",
            ]
        else:
            dl_age_opt = (
                f"--max-age-days {max_age_days} " if max_age_days is not None else ""
            )
            dl_notify_opt = "--notify-complete " if refresh_only else ""
            pipeline_steps = [
                f"cd {base}",
                get_ids_cmd,
                f"downloader {dl_age_opt}{dl_notify_opt}{csv_file} {canonical}",
            ]
            if not refresh_only:
                pipeline_steps.append(f"uploader {csv_file} {canonical}")
                # SDC sync is the final step of every upload run: it reads
                # the per-item sdc.json (staged by get-ids-es) and
                # upload-result.json (written by uploader) sidecars and
                # posts MediaInfo statements to Commons. Skipped for
                # refresh_only because no upload phase ran — there's no
                # upload-result.json to drive from.
                pipeline_steps.append(
                    f"sdc-sync --partner {canonical} --ids-file {csv_file}"
                )
        target_steps = " && ".join(pipeline_steps)
        target_blocks.append(
            f"{label_export}; {{ {target_steps}; }}"
            f" || {{ {notify_fail_cmd} >/dev/null 2>&1 || true; }}"
        )
    pipeline_cmd = f"{setup} && {{ {'; '.join(target_blocks)}; }}"

    if slack_token:

        def _target_label(c: str, insts: tuple[str, ...], col: str | None) -> str:
            """Format a batch target as the pipe-separated string shown in Slack.

            For a combined-institution target (multiple institutions from a
            single QID under one hub), shows the first institution + a
            ``(+N more)`` hint — full list would blow up the Slack message
            width for QIDs with many sub-institutions.
            """
            if col:
                # Collection-level is always single-institution by construction.
                return f"`{c}|{insts[0]}|{col}`"
            if len(insts) == 1:
                return f"`{c}|{insts[0]}`"
            if insts:
                return f"`{c}|{insts[0]} (+{len(insts) - 1} more)`"
            return f"`{c}`"

        single_item_targets = [
            (c, lbl, did) for c, _, lbl, did, _ in targets if did is not None
        ]
        batch_targets = [
            (c, i, lbl, col) for c, i, lbl, did, col in targets if did is None
        ]
        refresh_suffix = ""
        if refresh_only:
            age_note = f">{max_age_days}d" if max_age_days is not None else ">365d"
            refresh_suffix = f" (ID generation → download, refreshing files {age_note})"
        if single_item_targets and not batch_targets:
            # All single-item targets: use the eligibility-confirmed format.
            item_descs = [f"`{did}` ({c})" for c, _, did in single_item_targets]
            if refresh_only:
                msg = f"✅ {', '.join(item_descs)} — eligible, download refresh starting{refresh_suffix}."
            elif sdc_only:
                msg = (
                    f"✅ {', '.join(item_descs)} — eligible,"
                    " SDC-only backfill starting."
                )
            else:
                msg = (
                    f"✅ {', '.join(item_descs)} — eligible,"
                    " download + upload + SDC starting."
                )
        elif batch_targets and not single_item_targets:
            # All batch targets (hub, institution, or collection).
            batch_labels = [_target_label(c, i, col) for c, i, _, col in batch_targets]
            if refresh_only:
                msg = f"▶ Launching `{session_name}` refresh: {', '.join(batch_labels)}{refresh_suffix}."
            elif sdc_only:
                msg = (
                    f"▶ Launching `{session_name}` SDC backfill:"
                    f" {', '.join(batch_labels)} (ID generation → SDC)."
                )
            else:
                msg = (
                    f"▶ Launching `{session_name}` pipeline: {', '.join(batch_labels)}"
                    " (ID generation → download → upload → SDC)."
                )
        else:
            # Mixed: list all targets with a note distinguishing items from batches.
            all_descs = []
            for c, i, lbl, did, col in targets:
                if did is not None:
                    all_descs.append(f"`{did[:8]}…` ({c}, item)")
                else:
                    all_descs.append(_target_label(c, i, col))
            if refresh_only:
                msg = f"▶ Launching `{session_name}` refresh: {', '.join(all_descs)}{refresh_suffix}."
            elif sdc_only:
                msg = (
                    f"▶ Launching `{session_name}` SDC backfill:"
                    f" {', '.join(all_descs)} (ID generation → SDC)."
                )
            else:
                msg = (
                    f"▶ Launching `{session_name}` pipeline: {', '.join(all_descs)}"
                    " (ID generation → download → upload → SDC)."
                )
        try:
            post_message(slack_token, msg)
        except Exception as e:
            logging.warning("Slack notification failed: %s", e)

    print(f"Launching {session_name} pipeline...")
    # Stage pipeline_cmd to a script file on EC2 and launch tmux against it
    # rather than inlining as a `"PIPELINE_CMD"` argument to tmux. The
    # inline form ran into SSM's per-command size limit ("command too long"
    # on a 22-target batch — single-quoted institution names plus the
    # shlex.quote multiplier add up fast). stage_and_launch_tmux base64-
    # encodes the script and decodes it on the instance, keeping the SSM
    # payload compact regardless of pipeline length. See ssm.py for the
    # full rationale.
    out = ""
    try:
        out = stage_and_launch_tmux(
            ssm,
            script=pipeline_cmd,
            session_name=session_name,
            cwd="/home/ec2-user/ingest-wikimedia/",
        )
    except Exception as e:
        _slack_fail(
            response_url, f"⚠️ Failed to launch tmux session `{session_name}`: {e}"
        )
    if "SESSION_STARTED" not in out:
        _slack_fail(
            response_url,
            f"⚠️ `{session_name}` failed to start — tmux could not create session."
            " Check the GitHub Actions run for details.",
        )
    print(f"Session {session_name} confirmed running.")


if __name__ == "__main__":
    main()
