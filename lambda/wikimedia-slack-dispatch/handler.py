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
  /wikimedia-upload sdc <target> [<target> ...]
                     — dispatches wikimedia-launch.yml with sdc_only=true to
                       re-enumerate IDs and run the SDC sync phase only,
                       skipping download + upload. Used to recover from
                       aborted SDC syncs.

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
import shlex
import time
import urllib.error
import urllib.parse
import urllib.request

from ingest_wikimedia.partners import is_dpla_id, is_wikidata_id, resolve_slug


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


def _is_dispatch_timeout(exc: BaseException) -> bool:
    """True if ``exc`` indicates the dispatch may have arrived late, not failed.

    ``urllib.request.urlopen()`` with a ``timeout=`` argument doesn't propagate
    a raw ``TimeoutError`` — the underlying ``socket.timeout`` (aliased to
    ``TimeoutError`` in Python 3.10+) is wrapped inside ``urllib.error.URLError``
    via ``URLError.reason``. Catching only ``TimeoutError`` would therefore miss
    the realistic slow-but-delivered case and route it to the misleading
    "internal error" branch.
    """
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, urllib.error.URLError):
        return isinstance(getattr(exc, "reason", None), TimeoutError)
    return False


def _launch_with_targets(
    token: str,
    repo: str,
    response_url: str,
    targets: list[str],
    extra_inputs: dict,
    success_text_fn,
) -> dict:
    """Dispatch ``wikimedia-launch.yml`` for a list of validated targets.

    Shared by the ``sdc`` and ``refresh`` subcommands (and any future variant
    that just toggles a launch.yml input). ``extra_inputs`` is merged with the
    standard partner/response_url/concurrency_key payload; ``success_text_fn``
    receives the comma-joined ```backtick``-wrapped targets_display.
    """
    if not targets:
        return _slack_reply("No valid targets provided.", ephemeral=True)
    partner_input = shlex.join(targets)
    targets_display = ", ".join(f"`{t}`" for t in targets)
    # Keep the concurrency group name well under GitHub's 400-char limit by
    # hashing the full partner string down to a 16-char hex prefix.
    concurrency_key = hashlib.sha256(partner_input.encode()).hexdigest()[:16]
    inputs = {
        "partner": partner_input,
        "response_url": response_url,
        "concurrency_key": concurrency_key,
        **extra_inputs,
    }
    return _dispatch_and_reply(
        token, repo, "wikimedia-launch.yml", inputs, success_text_fn(targets_display)
    )


def _dispatch_and_reply(
    token: str,
    repo: str,
    workflow: str,
    inputs: dict,
    success_text: str,
    *,
    ephemeral: bool = False,
) -> dict:
    try:
        status = _dispatch_workflow(token, repo, workflow, inputs)
    except urllib.error.HTTPError as e:
        logging.error("GitHub API error: HTTP %s", e.code)
        return _slack_reply(
            f"Failed to trigger workflow (HTTP {e.code}).", ephemeral=ephemeral
        )
    except (TimeoutError, urllib.error.URLError) as e:
        # A slow GitHub API response often still results in a successful
        # dispatch — telling the user "internal error" in that case is
        # misleading, because the workflow may already be queued. Use the
        # softer message only for timeout-shaped errors; other URL errors
        # (DNS failure, connection refused, etc.) still indicate the
        # dispatch did not happen and should keep the hard-failure wording.
        if _is_dispatch_timeout(e):
            logging.warning(
                "Timeout waiting for GitHub dispatch response (%s)", workflow
            )
            return _slack_reply(
                "GitHub API was slow — workflow may have been dispatched anyway."
                " Check #tech-alerts in ~2 minutes or the Actions tab to confirm.",
                ephemeral=ephemeral,
            )
        logging.exception("URL error dispatching workflow")
        return _slack_reply(
            "Failed to trigger workflow due to an internal error.", ephemeral=ephemeral
        )
    except Exception:
        logging.exception("Unexpected error dispatching workflow")
        return _slack_reply(
            "Failed to trigger workflow due to an internal error.", ephemeral=ephemeral
        )
    text = (
        success_text
        if status == 204
        else f"Unexpected response from GitHub (HTTP {status})"
    )
    return _slack_reply(text, ephemeral=ephemeral)


def _parse_positive_int(value: str) -> tuple[int, dict | None]:
    """Parse a positive integer. Returns (n, None) on success or (0, error_reply)."""
    try:
        n = int(value)
        if n <= 0:
            raise ValueError
    except ValueError:
        return 0, _slack_reply(
            f"`{value}` is not a valid number of days. Provide a positive integer.",
            ephemeral=True,
        )
    return n, None


def _validate_launch_targets(
    tokens: list[str],
) -> tuple[list[str], dict | None]:
    """
    Validate a list of target tokens (hub slugs, DPLA IDs, QIDs, pipe-separated).
    Returns (launch_targets, None) on success, or ([], error_reply) on the first
    invalid token.
    """
    seen_tokens: set[str] = set()
    launch_targets: list[str] = []
    for token in tokens:
        if is_wikidata_id(token):
            if token in seen_tokens:
                return [], _slack_reply(
                    f"Target `{token}` appears more than once.", ephemeral=True
                )
            seen_tokens.add(token)
            launch_targets.append(token)
        elif is_dpla_id(token):
            normalised = token.lower()
            if normalised in seen_tokens:
                return [], _slack_reply(
                    f"Target `{normalised}` appears more than once.", ephemeral=True
                )
            seen_tokens.add(normalised)
            launch_targets.append(normalised)
        else:
            pipe_count = token.count("|")
            if pipe_count > 2:
                return [], _slack_reply(
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
                return [], _slack_reply("Institution cannot be empty.", ephemeral=True)
            if collection is not None and not collection:
                return [], _slack_reply("Collection cannot be empty.", ephemeral=True)
            canonical = resolve_slug(hub_part)
            if canonical is None:
                return [], _slack_reply(
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
                return [], _slack_reply(
                    f"Target `{target_str}` appears more than once.", ephemeral=True
                )
            seen_tokens.add(target_str)
            launch_targets.append(target_str)
    return launch_targets, None


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
                "To stop a session: `/wikimedia-upload kill <label> [<label> ...]`\n"
                "To retry transient failures: `/wikimedia-upload retry <days> [<partner>]`\n"
                "To refresh S3 files: `/wikimedia-upload refresh <target> [<target> ...] <days>`\n"
                "To re-run only the SDC sync phase: `/wikimedia-upload sdc <target> [<target> ...]`\n"
                "To maintain existing files in place (no uploads):"
                " `/wikimedia-upload maintain <target> [<target> ...]`",
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

        # Retry subcommand: /wikimedia-upload retry <days> [<partner>]
        # Scans the last DAYS days of upload/download logs for transient failures
        # and re-runs the affected items.
        if tokens[0] == "retry":
            retry_tokens = tokens[1:]
            if not retry_tokens:
                return _slack_reply(
                    "Usage: `/wikimedia-upload retry <days> [<partner>]`\n"
                    "Scans the last DAYS days of logs and re-runs transiently failed items.\n"
                    "Example: `/wikimedia-upload retry 7` or `/wikimedia-upload retry 14 nara`",
                    ephemeral=True,
                )
            days, err = _parse_positive_int(retry_tokens[0])
            if err is not None:
                return err
            if len(retry_tokens) > 2:
                return _slack_reply(
                    "Too many arguments. Usage: `/wikimedia-upload retry <days> [<partner>]`",
                    ephemeral=True,
                )
            retry_partner = ""
            if len(retry_tokens) > 1:
                hub = retry_tokens[1]
                canonical = resolve_slug(hub)
                if canonical is None:
                    return _slack_reply(
                        f"Unknown hub: `{hub}`. Check the hub slug and try again.",
                        ephemeral=True,
                    )
                retry_partner = canonical
            return _dispatch_and_reply(
                gh_token,
                repo,
                "wikimedia-retry.yml",
                {
                    "days": str(days),
                    "partner": retry_partner,
                    "response_url": response_url,
                },
                f"Scanning the last {days} day{'s' if days != 1 else ''} of logs"
                f" for retryable failures{f' for `{retry_partner}`' if retry_partner else ''}"
                " — results will post to #tech-alerts shortly.",
                ephemeral=True,
            )

        # SDC subcommand: /wikimedia-upload sdc <target> [<target> ...]
        # Re-enumerates IDs and runs sdc-sync; skips download + upload phases.
        # Used to recover SDC sync runs that were aborted before completion.
        if tokens[0] == "sdc":
            sdc_tokens = tokens[1:]
            if not sdc_tokens:
                return _slack_reply(
                    "Usage: `/wikimedia-upload sdc <target> [<target> ...]`\n"
                    "Re-enumerates IDs and runs the SDC sync phase only"
                    " (skips download + upload).\n"
                    'Example: `/wikimedia-upload sdc "nara|William J. Clinton Library"`',
                    ephemeral=True,
                )
            sdc_targets, err = _validate_launch_targets(sdc_tokens)
            if err is not None:
                return err
            return _launch_with_targets(
                gh_token,
                repo,
                response_url,
                sdc_targets,
                {"sdc_only": "true"},
                lambda targets_display: (
                    f"Launching SDC-only sync for {targets_display}"
                    " — confirmation will post to #tech-alerts shortly."
                ),
            )

        # Maintain subcommand: /wikimedia-upload maintain <target> [<target> ...]
        # Re-links + SDC-syncs the EXISTING Commons files of a (possibly
        # no-longer-participating) hub/institution in place. No download, no
        # upload, no new File pages — so upload-ineligible targets are allowed.
        if tokens[0] == "maintain":
            maintain_tokens = tokens[1:]
            # An optional mode modifier follows `maintain`:
            #   count — pre-flight sizing (lite re-link, writes nothing)
            #   lite  — quick no-download sidecar route (SDC-in-place + rename)
            #   (none) — DEFAULT hash route: download + content reconcile + SDC
            mode = (
                maintain_tokens[0]
                if maintain_tokens and maintain_tokens[0] in ("count", "lite")
                else None
            )
            if mode:
                maintain_tokens = maintain_tokens[1:]
            count_only = mode == "count"
            lite = mode == "lite"
            if not maintain_tokens:
                return _slack_reply(
                    "Usage: `/wikimedia-upload maintain [lite|count] <target> [<target> ...]`\n"
                    "Reconciles files already on Commons for a hub or institution"
                    " (never creates new files); works for hubs no longer"
                    " participating in uploads.\n"
                    "Default (hash): downloads media and content-reconciles —"
                    " re-links drifted files + overwrites changed bytes + SDC.\n"
                    "`lite`: quick no-download route — SDC-in-place + name-drift"
                    " rename only.\n"
                    "`count`: size the re-link without writing anything.\n"
                    'Example: `/wikimedia-upload maintain "georgia|Atlanta History Center"`\n'
                    "Example: `/wikimedia-upload maintain lite digitalnc`\n"
                    "Example: `/wikimedia-upload maintain count digitalnc`",
                    ephemeral=True,
                )
            maintain_targets, err = _validate_launch_targets(maintain_tokens)
            if err is not None:
                return err
            inputs = {"maintain": "true"}
            if count_only:
                inputs["count_only"] = "true"
            if lite:
                inputs["lite"] = "true"
            if count_only:
                action = "maintain pre-flight sizing (count-only, writes nothing)"
            elif lite:
                action = "lite maintain (SDC-in-place + rename, no download)"
            else:
                action = "maintain (download + content reconcile + SDC)"
            return _launch_with_targets(
                gh_token,
                repo,
                response_url,
                maintain_targets,
                inputs,
                lambda targets_display: (
                    f"Launching {action} for"
                    f" {targets_display} — confirmation will post to"
                    " #tech-alerts shortly."
                ),
            )

        # Refresh subcommand: /wikimedia-upload refresh <target> [<target> ...] <days>
        # Re-downloads files older than DAYS days without running the uploader.
        if tokens[0] == "refresh":
            refresh_tokens = tokens[1:]
            if len(refresh_tokens) < 2:
                return _slack_reply(
                    "Usage: `/wikimedia-upload refresh <target> [<target> ...] <days>`\n"
                    "Re-downloads files already in S3 that are older than DAYS days,"
                    " without re-uploading to Commons.\n"
                    "Example: `/wikimedia-upload refresh ohio 90`\n"
                    'Example: `/wikimedia-upload refresh "indiana|Indiana State Library" 30`\n'
                    "Example: `/wikimedia-upload refresh bpl pa ohio 30`",
                    ephemeral=True,
                )
            days, err = _parse_positive_int(refresh_tokens[-1])
            if err is not None:
                return err
            refresh_targets, err = _validate_launch_targets(refresh_tokens[:-1])
            if err is not None:
                return err
            return _launch_with_targets(
                gh_token,
                repo,
                response_url,
                refresh_targets,
                {"max_age_days": str(days), "refresh_only": "true"},
                lambda targets_display: (
                    f"Launching download refresh for {targets_display}"
                    f" — re-downloading files older than {days}"
                    f" day{'s' if days != 1 else ''}, no upload."
                    " Confirmation will post to #tech-alerts shortly."
                ),
            )

        # Launch subcommand: /wikimedia-upload <target> [<target> ...]
        # QIDs and DPLA IDs are passed through; the launch script resolves them.
        # Hub-based targets are validated here for fast Slack feedback.
        launch_targets, err = _validate_launch_targets(tokens)
        if err is not None:
            return err

        partner_input = shlex.join(launch_targets)
        targets_display = ", ".join(f"`{t}`" for t in launch_targets)
        # Keep the concurrency group name well under GitHub's 400-char limit by
        # hashing the full partner string down to a 16-char hex prefix.
        concurrency_key = hashlib.sha256(partner_input.encode()).hexdigest()[:16]

        try:
            status = _dispatch_workflow(
                gh_token,
                repo,
                "wikimedia-launch.yml",
                {
                    "partner": partner_input,
                    "response_url": response_url,
                    "concurrency_key": concurrency_key,
                },
            )
        except urllib.error.HTTPError as e:
            logging.error("GitHub API error: HTTP %s", e.code)
            return _slack_reply(
                f"Failed to launch pipeline for {targets_display} (HTTP {e.code})."
            )
        except (TimeoutError, urllib.error.URLError) as e:
            if _is_dispatch_timeout(e):
                logging.warning(
                    "Timeout waiting for GitHub dispatch response for targets: %s",
                    targets_display,
                )
                return _slack_reply(
                    f"GitHub API was slow — pipeline for {targets_display} may have been dispatched anyway. "
                    "Check #tech-alerts in ~2 minutes or the Actions tab to confirm."
                )
            logging.exception("URL error dispatching workflow")
            return _slack_reply(
                f"Failed to launch pipeline for {targets_display} due to an internal error."
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
