#!/usr/bin/env python3
"""Advance the DPLA duplicate "moving window" to keep a bounded number of
DPLA duplicate files visible in Commons ``Category:Duplicate``.

Background
----------
DPLA tags duplicate uploads for deletion. ``Category:Duplicate`` is patrolled
by Commons admins (speedy-deletion criterion F8); flooding it with tens of
thousands of DPLA files at once overwhelms them. Instead, every DPLA duplicate
is tagged with ``{{DPLA duplicate|1=<correct file>|ts=<sortkey>|reason=<why>}}``
which:

  * places the file in ``Category:DPLA duplicates for deletion`` (a private
    backlog, sorted oldest-first by the ``ts`` sortkey) when its ``ts`` is
    NOT below the moving window, and
  * transcludes ``{{Duplicate|1=<correct file>|2=<reason>}}`` (emitting the
    real ``Category:Duplicate`` + F8, with the duplicate's target and reason)
    when its ``ts`` IS below the moving window.

  A tag with no ``ts`` fails closed: it lands in the backlog without a sort
  key and is skipped by this job (non-integer sort key), so it is never
  released.

The window value lives at ``Template:DPLA duplicate/moving window`` — a single
integer. A file is "released" (visible to admins) iff ``ts < window``. Because
the release condition is centralised in one transcluded value, revealing more
files costs exactly ONE edit (bumping the window) plus invisible re-parses;
no per-file edits clutter file histories.

This job (run hourly as a scheduled GitHub Action) is the only active piece:
it advances the window just enough to keep ~``TARGET`` DPLA files visible as
admins drain them, oldest-first, and never blocks the ingest pipeline.

Algorithm (per run)
-------------------
1. Read the current window value (fail-closed to 0 → nothing released).
2. Count visible DPLA files = members of ``Category:Duplicate`` whose filename
   carries the `` - DPLA - `` token. That category is only a few hundred files,
   so this is one or two API pages. Counting by filename (rather than by a
   DPLA-specific tracking category) is deliberate: the upload bot also tags
   some DPLA duplicates with a plain ``{{Duplicate}}`` — those carry no
   DPLA-specific category but the same filename token — so the filename scan
   captures BOTH tagging routes and the two mechanisms share one ~TARGET
   budget instead of each filling it independently. Deleted files leave the
   category, so this is "currently visible to admins". (Known gap: a legacy
   DPLA file renamed to drop the `` - DPLA - `` token won't be counted; none
   exist on the current backlog and the drift is tolerable.)
3. If ``visible >= TARGET`` → nothing to do.
4. Otherwise fetch the ``TARGET - visible`` oldest files from the backlog
   category (which, being mutually exclusive with the release branch, holds
   only NOT-yet-released files) with their integer sortkeys.
5. Set the new window to ``(largest fetched sortkey) + 1`` so exactly those
   files cross the threshold. Monotonic by construction (all fetched keys are
   ``>=`` the old window); guarded anyway.
6. Save the window (one edit) and force a prompt re-parse of the just-released
   files via ``action=purge&forcelinkupdate=1`` (invisible — no revision, no
   RecentChanges/watchlist entry) so the next run's count is accurate.

Idempotent: a run that finds the category full, or computes no advance, makes
no edits. Re-running is always safe.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass

import pywikibot
from pywikibot.data import api

WINDOW_PAGE = "Template:DPLA duplicate/moving window"
BACKLOG_CATEGORY = "Category:DPLA duplicates for deletion"
DUPLICATE_CATEGORY = "Category:Duplicate"
# Substring that marks a Commons filename as a DPLA upload (e.g.
# "Some title - DPLA - <id> (page 1).jpg"). Used to pick DPLA files out of the
# shared, sitewide Category:Duplicate. Spaces are significant — they keep the
# token from matching an incidental "DPLA" elsewhere in a title.
DPLA_FILENAME_TOKEN = " - DPLA - "
DEFAULT_TARGET = 100
FILE_NAMESPACE = 6

# The window value is stored inside an <includeonly> block so the page can
# also carry human-readable <noinclude> documentation without leaking it into
# the transcluded expression. Match the first run of digits inside it.
_WINDOW_VALUE_RE = re.compile(r"<includeonly>\s*(\d+)\s*</includeonly>", re.IGNORECASE)


@dataclass(frozen=True)
class ReleasePlan:
    """The outcome of :func:`compute_release_plan`.

    ``new_window`` is the value to write (equal to the current window when
    nothing should change). ``keys_to_reveal`` are the sortkeys that will
    cross the threshold — used only for the follow-up purge and logging.
    """

    new_window: int
    keys_to_reveal: list[int]

    @property
    def is_noop(self) -> bool:
        return not self.keys_to_reveal


def parse_window_value(text: str | None) -> int:
    """Extract the integer window value from the window page's wikitext.

    Fail-closed: missing page, empty text, or no parseable value all yield
    ``0`` — with ``0`` no positive sortkey satisfies ``ts < window``, so
    nothing is released. This is the safe default that prevents an
    uninitialised or corrupted window page from dumping the whole backlog
    into ``Category:Duplicate``.
    """
    if not text:
        return 0
    m = _WINDOW_VALUE_RE.search(text)
    if not m:
        # Fall back to a bare integer body (e.g. a hand-edited page with no
        # includeonly wrapper), still fail-closed if nothing numeric is found.
        stripped = text.strip()
        return int(stripped) if stripped.isdigit() else 0
    return int(m.group(1))


def render_window_value(value: int, doc_note: str = "") -> str:
    """Render the window page wikitext for ``value``.

    The value sits in an <includeonly> block (so transclusion yields exactly
    the integer) followed by a <noinclude> note pointing maintainers at the
    template docs. ``doc_note`` lets callers/tests override the note.
    """
    note = doc_note or (
        "This page holds a single integer: the DPLA duplicate moving-window "
        "cutoff (a file is released into [[Category:Duplicate]] when its "
        "sortkey is less than this value). It is maintained automatically by "
        "the dpla-dup-window job; do not edit by hand. See "
        "[[Template:DPLA duplicate/doc]]."
    )
    return f"<includeonly>{value}</includeonly><noinclude>\n{note}\n</noinclude>"


def compute_release_plan(
    current_window: int,
    visible_count: int,
    target: int,
    unrevealed_keys_ascending: list[int],
) -> ReleasePlan:
    """Pure core: decide the new window value and which keys it releases.

    ``unrevealed_keys_ascending`` must be the oldest-first sortkeys of files
    NOT yet released (i.e. drawn from the backlog category, which is mutually
    exclusive with the release branch). The caller fetches at most the deficit,
    but this function tolerates a longer list and only ever releases up to the
    deficit.

    Returns a no-op plan (``new_window == current_window``, empty keys) when
    the category is already at/above target or there are no unrevealed files.
    """
    if visible_count >= target:
        return ReleasePlan(current_window, [])
    deficit = target - visible_count
    take = unrevealed_keys_ascending[:deficit]
    if not take:
        return ReleasePlan(current_window, [])
    # Releasing exactly ``take`` means the window must sit just past the
    # largest key in ``take``. ``take`` is the smallest ``deficit`` unrevealed
    # keys, so every key <= max(take) is either in ``take`` or already
    # released — hence window = max(take) + 1 reveals precisely ``take``.
    candidate = max(take) + 1
    # Monotonic guard: never move the window backwards. With keys drawn from
    # the backlog (all >= current_window) this is automatic, but a stale read
    # or an out-of-contract key list must not regress the window.
    new_window = max(candidate, current_window)
    if new_window == current_window:
        return ReleasePlan(current_window, [])
    return ReleasePlan(new_window, take)


def is_dpla_duplicate_title(title: str) -> bool:
    """True if ``title`` is a DPLA-uploaded file, by the `` - DPLA - `` token.

    The mechanism-agnostic signal for "this file is ours": it matches DPLA
    duplicates tagged by the moving-window template AND those tagged directly
    with a plain ``{{Duplicate}}`` by the upload bot (which carry no
    DPLA-specific category). Pure so it can be unit-tested without a site.
    """
    return DPLA_FILENAME_TOKEN in title


def _count_visible(site: pywikibot.site.APISite) -> int:
    """Count DPLA files currently visible in ``Category:Duplicate``.

    Enumerates the (few-hundred-member) sitewide duplicate category and counts
    File-namespace members whose title carries the `` - DPLA - `` token — see
    :func:`is_dpla_duplicate_title` for why the filename token, not a
    DPLA-specific category, is the correct budget signal.
    """
    cat = pywikibot.Category(site, DUPLICATE_CATEGORY)
    return sum(
        1
        for page in cat.members(namespaces=[FILE_NAMESPACE])
        if is_dpla_duplicate_title(page.title(with_ns=False))
    )


def _fetch_oldest_unrevealed(
    site: pywikibot.site.APISite, limit: int
) -> list[tuple[int, str]]:
    """Return up to ``limit`` (sortkey, title) pairs from the backlog category,
    oldest sortkey first.

    Reads ``sortkeyprefix`` (the human-readable sortkey we set, e.g.
    ``00000000000000012345``) directly from the API so the window value can be
    computed from real keys rather than inferred from order. Skips any member
    whose sortkey prefix isn't a plain integer (defensive against a
    hand-tagged page).

    ``limit`` is clamped to the API's single-request ceiling (500 for
    non-bot; we stay at the universal floor rather than assume the bot
    flag). A ``--target`` large enough to exceed it just fetches 500 this
    run and catches up next run — the window is self-correcting.
    """
    request = api.Request(
        site=site,
        parameters={
            "action": "query",
            "list": "categorymembers",
            "cmtitle": BACKLOG_CATEGORY,
            "cmprop": "title|sortkeyprefix",
            "cmsort": "sortkey",
            "cmdir": "ascending",
            "cmnamespace": FILE_NAMESPACE,
            "cmlimit": str(min(limit, 500)),
        },
    )
    data = request.submit()
    out: list[tuple[int, str]] = []
    for member in data.get("query", {}).get("categorymembers", []):
        prefix = (member.get("sortkeyprefix") or "").strip()
        if not prefix.isdigit():
            logging.warning(
                "Skipping %r: non-integer sortkey prefix %r",
                member.get("title"),
                prefix,
            )
            continue
        out.append((int(prefix), member["title"]))
    return out


def _purge_forcelinkupdate(site: pywikibot.site.APISite, titles: list[str]) -> None:
    """Purge ``titles`` with ``forcelinkupdate`` so their categorylinks update
    promptly (plain purge does NOT rewrite categorylinks). Purges are not
    edits: no revision, no RecentChanges, no watchlist noise, no edit-budget
    cost. Rate-limited (~1/s) so we chunk conservatively.
    """
    if not titles:
        return
    for i in range(0, len(titles), 20):
        chunk = titles[i : i + 20]
        try:
            api.Request(
                site=site,
                parameters={
                    "action": "purge",
                    "forcelinkupdate": "1",
                    "titles": "|".join(chunk),
                },
            ).submit()
            logging.info("Purged (forcelinkupdate) %d file(s).", len(chunk))
        except Exception as ex:
            # Best-effort: the window edit that already committed will re-parse
            # these files via its own refreshLinks fan-out (just less
            # promptly), and the next run re-reads the live count — so a purge
            # failure must neither abort the run (the window has already
            # advanced) nor drop the remaining chunks.
            logging.warning(
                "Purge (forcelinkupdate) failed for %d file(s) (%s); "
                "continuing — release still lands via the window edit's "
                "fan-out and the count self-corrects next run.",
                len(chunk),
                ex,
            )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--target",
        type=int,
        default=DEFAULT_TARGET,
        help=f"Desired number of DPLA files visible in Category:Duplicate (default {DEFAULT_TARGET}).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and log the plan but make no edits or purges.",
    )
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    site = pywikibot.Site()
    site.login()

    window_page = pywikibot.Page(site, WINDOW_PAGE)
    current_window = parse_window_value(
        window_page.text if window_page.exists() else ""
    )
    visible = _count_visible(site)
    logging.info(
        "Current window=%d; DPLA files visible in Category:Duplicate=%d; target=%d",
        current_window,
        visible,
        args.target,
    )

    if visible >= args.target:
        logging.info("At/above target; nothing to release.")
        return 0

    deficit = args.target - visible
    oldest = _fetch_oldest_unrevealed(site, deficit)
    plan = compute_release_plan(
        current_window, visible, args.target, [k for k, _ in oldest]
    )
    if plan.is_noop:
        logging.info("No unrevealed backlog files to release; window unchanged.")
        return 0

    reveal_keys = set(plan.keys_to_reveal)
    titles = [title for key, title in oldest if key in reveal_keys]
    logging.info(
        "Releasing %d file(s): window %d → %d.",
        len(plan.keys_to_reveal),
        current_window,
        plan.new_window,
    )
    if args.dry_run:
        for key, title in oldest:
            if key in reveal_keys:
                logging.info("  would release: %s (sortkey %d)", title, key)
        logging.info("Dry run: no edits or purges made.")
        return 0

    window_page.text = render_window_value(plan.new_window)
    window_page.save(
        summary=(
            f"Advance DPLA duplicate moving window {current_window} → "
            f"{plan.new_window} (release {len(plan.keys_to_reveal)} file(s) to "
            f"Category:Duplicate; refill to {args.target})"
        ),
        minor=False,
        bot=True,
    )
    _purge_forcelinkupdate(site, titles)
    logging.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
