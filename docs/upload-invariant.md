# The DPLA → Wikimedia Commons upload invariant

This document is the canonical statement of what "correct" means for
the upload pipeline. It exists because the correctness criterion is
otherwise scattered across `tools/uploader.py`, `_resolve_hash_drift`,
and the redirect-handling branches — each of which enforces a slice of
the invariant, but none of which states the whole invariant on its
own. A reader vetting an outcome (or considering a change) should
start here.

Every function docstring and case-label comment in the drift /
redirect code path refers back to this document. If you change
runtime behaviour of the upload flow, this document is the checklist
your change must satisfy.

## The goal, in one sentence

**For every DPLA item, its content must be represented on Commons and
discoverable at the title `get_page_title(dpla_id, …)` produces.**

That is the unchanging goal. What changed (PR C+D) is *how* we satisfy
it when the same bytes are involved more than once.

## The uniqueness constraint (PR C+D)

**The uploader never creates a second Commons file for a SHA1 that
already exists.** (This governs what the bot *uploads* going forward; it
is not a claim that no two files on Commons share a SHA1 today —
pre-existing legacy duplicates are out of scope, see below.)

We therefore no longer satisfy the goal by uploading a byte-identical
second file. Instead, when our S3 source's SHA1 already exists on
Commons we:

1. **Centralize** the SHA1 to ONE canonical file. The canonical file is
   the **earliest existing upload** carrying that SHA1. It accumulates
   every contributing DPLA item's structured data (SDC): each
   contributing item's `P760` (DPLA ID) claim is merged onto the
   canonical MediaInfo entity (add-only — no other contributor's
   statements are removed), with within-item page numbers stamped as
   `P304` qualifiers.
2. **Redirect** the other expected titles to it. Where a second file
   would otherwise have gone, we leave a `#REDIRECT [[File:<canonical>]]`.
   A redirect carries no media, so the one-SHA1-one-file constraint
   holds while the DPLA-ID title still resolves to the content.

**Scope: NEW UPLOADS ONLY.** This is the behaviour of the live
uploader going forward. There is no backfill sweep of already-uploaded
byte-identical pairs in this change.

**Commons-normalization equivalence** (unchanged). MediaWiki normalizes
some file formats on ingest (e.g. a PDF's trailing `\r` after `%%EOF`
is stripped). When we attempt to re-upload such a file, Commons responds
`fileexists-no-change` naming our intended title. That response is
authoritative: the invariant is satisfied at the correct title even
though our S3 SHA1 and Commons's stored SHA1 differ pre-normalization.
The uploader skips cleanly with `Result.UPLOAD_SKIPPED_COMMONS_DEDUP`
(via `_detect_commons_dedup_skip` / `_detect_commons_dedup_from_nochange_error`)
rather than treating it as FAILED.

## The four outcomes when our SHA1 is already on Commons

Once `find_file_by_hash` returns any existing file for our SHA1,
`process_file` NEVER uploads. It resolves to exactly one of:

1. **SKIP** — the file is already at the intended title (or at it modulo
   whitespace/underscore normalization → `ALREADY_CORRECT`). The goal is
   already met.
2. **MOVE / rename** (`DriftResolution.MOVED`) — the same content should
   simply live at the intended title, which is empty or a redirect to
   our own file. We rename the file into place (one file, one SHA1,
   canonical title).
3. **MERGE + REDIRECT** (`DriftResolution.MERGE_AND_REDIRECT`) — the SHA1
   match is legitimate **source duplication**: the same bytes appear at
   more than one position within an item (within-item), or across items
   / institutions (cross-item). We merge this item's SDC onto the
   earliest (canonical) file and leave a redirect at our intended title.
4. **HAND_FIX** — the bot can neither upload a duplicate nor safely pick a
   winner / clobber the occupant, so it records the case to the per-partner
   `hand-fix.jsonl` sidecar for a human and moves on
   (`Result.UPLOAD_HAND_FIX`). Two reasons occur:
   - **`rename_blocked`** (`DriftResolution.HAND_FIX`) — our SHA1 is at a
     wrong title and the intended title is occupied by a DIFFERENT file
     (different SHA1), or by a redirect to some third file, or the
     colliding file's DPLA ID could not be verified.
   - **`community_file`** — the SHA1 match is against a COMMUNITY file:
     its title lacks the DPLA/NARA shape (`- DPLA -` / `- NARA -`) **AND**
     its original uploader is not one of our bots (`DPLA_BOT_ACCOUNTS`). We
     never rename, merge onto, redirect, or migrate a community file, so it
     is handed to a human untouched. This case is decided in `process_file`
     (via `_is_community_file` / `_record_community_hand_fix_and_skip`)
     *before* any drift resolution runs, so no MOVE / MERGE_AND_REDIRECT can
     touch a community file.

On the fresh-upload path (`find_file_by_hash` returns `None`, i.e. our
SHA1 is not yet on Commons) the uploader uploads our bytes to the
intended title as before, including overwriting a pre-existing redirect
at that title (a human/bot redirect does not bind us — the intended
title is where our bytes belong).

## The mandatory investigation procedure

When investigating a suspected upload-flow bug — before drawing any
conclusion, offering any hypothesis, or proposing any fix — perform
these steps in order. **Do not skip ahead based on a code-narrative
hypothesis.**

1. Identify every DPLA ID and every Commons title involved.
2. For each Commons title: fetch `imageinfo` `sha1` + `size` from the
   Commons API (`action=query&prop=imageinfo&iiprop=sha1|size`), and note
   whether the page is a `#REDIRECT`.
3. For each DPLA ID: locate its S3 source at
   `s3://dpla-wikimedia/{partner}/images/{a}/{b}/{c}/{d}/{dpla_id}/{ordinal}_{dpla_id}`
   and read its `Metadata.sha1` via `aws s3api head-object` (or download
   the bytes and `shasum` them). **Do NOT infer the S3 SHA1 from a
   Commons file's SHA1 — that is circular reasoning.**
4. Build the matrix (rows = DPLA IDs, columns = intended title / observed
   Commons SHA1 (or redirect target) / S3 SHA1). Put the matrix in your
   writeup verbatim before drawing conclusions.
5. For each row ask: **is that DPLA ID's content discoverable at its
   intended title — either as the canonical file OR as a redirect to the
   one canonical file that holds its SHA1?** And separately: **does any
   SHA1 appear on more than one non-redirect Commons file?**
6. If every item is discoverable at its intended title AND no SHA1
   appears on two files → the invariant is satisfied.
7. If a SHA1 appears on two non-redirect files → that IS a bug now (a
   uniqueness-constraint violation); one of them should be a redirect to
   the other (the earliest). If an intended title neither holds the
   content nor redirects to the canonical file → THAT title is the bug.

## Failure modes — anti-patterns to actively guard against

- **"Upload a second copy because it's a different DPLA ID."** Two live
  DPLA IDs pointing at byte-identical content must resolve to ONE
  canonical file (earliest) carrying both IDs' SDC, with the other title
  redirecting to it — not two files sharing a SHA1. A second upload is a
  uniqueness-constraint violation.

- **"Clobber the occupant at the intended title."** When the intended
  title holds a DIFFERENT file, the bot must not overwrite it or tag it
  for deletion. Route to HAND_FIX and let a human decide.

- **"Emit `{{Duplicate}}` / tag-for-deletion."** The entire
  duplicate-tag / defer / drain apparatus is retired. Centralize +
  redirect is the mechanism now; a file redirect is not a deletion
  request and needs no admin action.

- **"Merge with reconciliation on the canonical file."** The merge is
  strictly ADD-ONLY (`reconcile=False`). A single item's sync must never
  strip another contributor's statements from a merged canonical file.

## Where the invariant is enforced in code

`get_page_title` (in `ingest_wikimedia/wikimedia.py`) defines "the
intended title" for a DPLA ID.

`process_file` (in `tools/uploader.py`) is the per-ordinal entry point.
When our SHA1 is already on Commons it dispatches to SKIP / MOVE /
MERGE+REDIRECT / HAND_FIX via `_resolve_hash_drift` (plus the within-item
`is_dup_sha1_sibling_at_expected_title` short-circuit); none of those
paths uploads. The merge is performed by `_merge_sdc_onto_canonical`
(which calls `tools.sdc_sync.merge_item_onto_canonical`) and the redirect
by `_create_redirect_to_canonical`. Hand-fix cases are written to the
sidecar by `_record_hand_fix_and_skip` (see
`ingest_wikimedia/hand_fix_sidecar.py`).

`_resolve_hash_drift` returns one of four `DriftResolution` sentinels;
see its docstring for the per-outcome mapping.

The docstrings on those functions link back to this document. If you
edit them, keep the link intact.
