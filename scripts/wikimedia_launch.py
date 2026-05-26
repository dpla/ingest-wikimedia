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
import re
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
)
from ingest_wikimedia.slack import post_message
from ingest_wikimedia.ssm import REGION, ssm_run

# Each ingest session peaks at ~300–500 MB; 30% of 7.6 GB leaves headroom for 4–5 concurrent sessions.
MEMORY_HEADROOM_PCT = 30

# Pre-compiled pattern for normalizing display names to tmux-safe label slugs.
_SLUG_RE = re.compile(r"[^a-z0-9-]")


def _slugify(name: str) -> str:
    """Normalize a display name to a tmux-safe slug (lowercase alphanumeric + hyphens)."""
    return _SLUG_RE.sub("", name.lower().replace(" ", "-"))


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
    args = parser.parse_args()

    force = args.force.lower() == "true"
    # GitHub Actions passes boolean inputs as the strings "true"/"false" — same as --force.
    refresh_only = args.refresh_only.lower() == "true"
    raw_url = args.response_url.strip()
    # Only accept genuine Slack response_url values — reject arbitrary POST targets.
    response_url = (
        raw_url if raw_url.startswith("https://hooks.slack.com/commands/") else ""
    )
    if raw_url and not response_url:
        print(f"Ignoring invalid response_url: {raw_url!r}", file=sys.stderr)
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
    # (canonical, institution_or_None, label, dpla_id_or_None, collection_or_None) tuples.
    # Dedup by full target string so the same hub may appear with different institutions
    # (e.g. two QIDs that both resolve into the same hub but different institutions).
    seen_target_strs: set[str] = set()
    seen_canonicals: dict[str, None] = {}  # insertion-ordered; for conflict detection
    seen_session_labels: dict[
        str, None
    ] = {}  # insertion-ordered; drives session naming
    targets: list[tuple[str, str | None, str, str | None, str | None]] = []
    # DPLA item ID tokens collected separately; resolved via EC2 before target building.
    dpla_id_tokens: list[str] = []
    # Per-target validation warnings; populated by _add_target and the conflict
    # check. If some targets are skipped but others remain valid, a summary is
    # posted to the caller before launch rather than aborting the whole run.
    skipped_warnings: list[str] = []

    def _add_target(
        canonical: str,
        institution: str | None,
        dpla_id: str | None = None,
        collection: str | None = None,
    ) -> None:
        if collection is not None and institution is None:
            skipped_warnings.append(
                f"Collection '{collection}' specified without an institution."
            )
            return
        if institution is not None:
            institution = institution.strip()
            if not institution:
                skipped_warnings.append(f"'{canonical}|': empty institution name.")
                return
        if dpla_id is None and canonical != "nara" and institution is None:
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
            target_str = f"{canonical}|{institution}|{collection}"
        elif institution is not None:
            target_str = f"{canonical}|{institution}"
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
        elif institution is not None:
            inst_label = _slugify(institution)
            if not inst_label:
                skipped_warnings.append(
                    f"'{canonical}|{institution}': institution name normalizes to an empty slug."
                )
                return
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
        targets.append((canonical, institution, label, dpla_id, collection))

    for token in target_tokens:
        if is_wikidata_id(token):
            resolved = resolve_wikidata_id(token)
            if not resolved:
                skipped_warnings.append(
                    f"Wikidata ID {token!r}: not found in institutions_v2.json."
                )
                continue
            for canonical, institution in resolved:
                _add_target(canonical, institution)
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
                _add_target(canonical, institution, collection=collection)
            else:
                _, institution = token_parts
                _add_target(canonical, institution)
        else:
            canonical = resolve_slug(token)
            if canonical is None:
                skipped_warnings.append(f"'{token}': unknown hub slug.")
                continue
            _add_target(canonical, None)

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
        for canonical, institution, label, dpla_id, collection in targets:
            if institution is None and dpla_id is None:
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
                inst_label = f"{canonical}+{_slugify(institution)}"
                conflicts_this = (
                    canonical in existing_labels
                    or inst_label in existing_labels
                    or label in existing_labels
                )
            elif dpla_id is None:
                # Institution-level requests conflict with:
                #   1. An existing hub-level session for the same hub
                #   2. An existing session for the exact same institution label
                #   3. Any existing collection-level session for the same institution
                #      (the collection is a strict subset; the institution run would
                #      duplicate work on those items, symmetric with collection→institution)
                # Two institution-level sessions for different institutions within
                # the same hub do NOT conflict.
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
    # notify_fail_cmd uses single-quoted Python inside a double-quoted tmux argument.
    # Single quotes are literal characters inside double-quoted bash strings, so the
    # inner Python code reaches the interpreter verbatim without needing any escaping.
    # `rc=$?` captures the failing step's exit code so the Slack message can decode
    # signals like 137 (SIGKILL / probable OOM) and 143 (SIGTERM).
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
    for idx, (canonical, institution, session_label, dpla_id, collection) in enumerate(
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
        elif canonical == "nara" and institution is None:
            get_ids_cmd = f"get-ids-nara > {csv_file}"
        else:
            get_ids_cmd = f"get-ids-es {canonical}"
            if institution is not None:
                get_ids_cmd += f" --institution {shlex.quote(institution)}"
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
            # SDC sync is the final step of every upload run: it reads the
            # per-item sdc.json (staged by get-ids-es) and upload-result.json
            # (written by uploader) sidecars and posts MediaInfo statements
            # to Commons. Skipped for refresh_only because no upload phase
            # ran — there's no upload-result.json to drive from.
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

        def _target_label(c: str, i: str | None, col: str | None) -> str:
            """Format a batch target as the pipe-separated string shown in Slack."""
            if col:
                return f"`{c}|{i}|{col}`"
            if i:
                return f"`{c}|{i}`"
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
    # Use double quotes around the pipeline so single-quoted institution names inside are
    # preserved when the outer shell (bash -c) processes the tmux command. The sentinel
    # echo runs immediately after tmux creates the detached session — before any pipeline
    # commands execute — so it is not subject to a race with fast-completing pipelines.
    tmux_cmd = (
        f"tmux new-session -d -s {shlex.quote(session_name)} -c /home/ec2-user/ingest-wikimedia/"
        f' "{pipeline_cmd}" && echo SESSION_STARTED'
    )
    out = ""
    try:
        out = ssm_run(ssm, tmux_cmd)
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
