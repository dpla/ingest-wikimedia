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

## The invariant, in one sentence

**For every DPLA item we upload, the SHA1 of the S3-staged source
bytes for that item must live at the Commons title
`get_page_title(dpla_id, …)` produces — modulo Commons's own
server-side ingest normalization for that file type.**

That is the entire correctness criterion. Everything else — Commons
redirects, `{{Duplicate}}` templates, "duplicate of" notices, human or
bot dedup judgments, intuitions about whether a state "looks like a
duplicate" — is downstream of this criterion and does not override
it.

**Commons-normalization equivalence.** MediaWiki normalizes some file
formats on ingest — e.g., PDFs with a trailing `\r` after `%%EOF`
have that byte stripped before storage. When we attempt to re-upload
such a file, Commons responds with `fileexists-no-change` naming our
intended target title. That response is authoritative: Commons is
telling us our upload equals what it already stores at the correct
title. The invariant is satisfied at the correct title even though
our S3 SHA1 and Commons's stored SHA1 differ pre-normalization.
Verified empirically byte-for-byte for the two files that motivated
this amendment (b2bc51b… and 8ac21ee786… ord 2). Trust Commons's
`fileexists-no-change`-with-target-title response as the invariant
check; skip cleanly with `Result.UPLOAD_SKIPPED_COMMONS_DEDUP`
rather than treating as FAILED.

## Corollaries (do not re-litigate these)

1. **Partner data is authoritative.** If a hub gives us two DPLA item
   records that point to byte-identical content, the correct Commons
   projection is two files at two DPLA-ID-suffixed titles holding the
   same bytes. **This is not a duplicate to fix; it is the invariant
   satisfied.** Users landing on either Commons title get the correct
   file for their DPLA ID; downstream systems that key on the DPLA ID
   (SDC sync, `dpla:` interwiki links, the DPLA API's Wikimedia
   participants records) resolve to the correct file each time.

2. **Pre-existing Commons redirects do not bind us.** A human editor
   (or an older bot) may have placed a `#REDIRECT [[File:X]]` on a
   Commons title with a summary like "duplicate of X" years before
   our current sync ran. That is a curatorial judgment about a
   partner-decided fact — one we do not defer to. If our intended
   title is such a redirect and our S3 source's SHA1 differs from the
   redirect target's SHA1, we overwrite the redirect and upload our
   bytes (this is what `_resolve_redirect_overwrite` does). If our
   S3 source's SHA1 matches the redirect target's SHA1 (the target
   IS our content, sitting at a different DPLA ID's canonical title
   because the partner emitted two DPLA IDs for the same source), we
   STILL overwrite: the invariant says our S3 SHA1 lives at OUR
   DPLA-ID-suffixed title, not "somewhere reachable via redirect from
   our title." See PR [#204](https://github.com/dpla/ingest-wikimedia/pull/204).

3. **Two Commons files with matching SHA1s is not evidence of a bug.**
   Stop pattern-matching on "same content at two titles" as a defect
   signal. The only correctness question is whether each intended
   DPLA-ID-suffixed title holds the SHA1 its S3 source dictates. If
   yes, and both DPLA IDs are live in the API, the two-files-same-SHA1
   state is corollary 1 — desired and correct.

## The mandatory investigation procedure

When investigating a suspected upload-flow bug — before drawing any
conclusion, offering any hypothesis, or proposing any fix — perform
these steps in order. **Do not skip ahead based on a code-narrative
hypothesis.**

1. Identify every DPLA ID and every Commons title involved.
2. For each Commons title: fetch `imageinfo` `sha1` + `size` from the
   Commons API (`action=query&prop=imageinfo&iiprop=sha1|size`).
3. For each DPLA ID: locate its S3 source at
   `s3://dpla-wikimedia/{partner}/images/{a}/{b}/{c}/{d}/{dpla_id}/{ordinal}_{dpla_id}`
   and read its `Metadata.sha1` via `aws s3api head-object` (or
   download the bytes and `shasum` them). **Do NOT infer the S3 SHA1
   from the Commons file's SHA1 — that is circular reasoning.**
4. Build the matrix (rows = DPLA IDs, columns = intended title /
   observed Commons SHA1 / S3 SHA1). Put the matrix in your writeup
   verbatim before drawing conclusions.
5. For each row, ask the only question that matters: **does the
   intended Commons title hold the SHA1 of that item's S3 source?**
6. If ALL rows are yes → the invariant is satisfied; the state is
   correct; the "bug" report is a corollary-1 or corollary-2 case.
   Explain the outcome as a faithful projection of partner data.
7. If any row is no → THAT specific mismatch is the bug. Report which
   intended title lacks the SHA1 its S3 source requires.

## Failure modes — anti-patterns to actively guard against

The invariant is violated by proposing "fixes" for corollary-1 or
corollary-2 states that seem intuitively wrong to someone unfamiliar
with the invariant. Some concrete anti-patterns:

- **"Skip the upload when the SHA1 already exists at another DPLA
  ID's title."** This proposal would leave the current DPLA ID's
  intended title without its S3 bytes — direct invariant violation.
  The corollary-1 two-files outcome is desired.

- **"Honor the redirect — don't overwrite it."** The intended title
  is where the bytes belong for this DPLA ID. A redirect at that
  title is a stale curatorial artifact from someone who didn't
  respect (or didn't know about) our invariant. Leaving the redirect
  in place means the intended title doesn't hold the required bytes —
  direct invariant violation.

- **"Add a `defense-in-depth` check that skips uploading when the
  target of the redirect has our SHA1."** Same as above wearing a
  different hat: skipping would leave our intended title as a
  redirect (or empty), which the invariant forbids.

- **"Tag the newly-uploaded file as a Commons-side duplicate of the
  other DPLA-ID's file."** Would flag the file for admin deletion,
  which if processed would empty the intended title and violate the
  invariant. And it misrepresents the state: it is not a Commons-side
  duplicate; it is a faithful projection of partner-side duplicate
  data. Two different facts.

## Concrete past incidents (illustrative)

- **2026-06-23** — `Block_Card_1440_Woodland_Avenue_-_DPLA_-_dd67df43….jpg`
  flagged as an apparent duplicate. Investigation: two DPLA IDs, both
  live; both S3 sources had the same SHA1; both intended Commons
  titles held that SHA1. Invariant satisfied. The apparent-duplicate
  was corollary 1 — faithful projection of partner data.

- **2026-07-02** — `Palo_Pinto_County_Star..._-_DPLA_-_343087804648...jpg`
  flagged after the bot overwrote a 2021-era human `#REDIRECT` and
  uploaded a file with content identical to the redirect target.
  Investigation: two DPLA IDs, both live; both S3 sources had the
  same SHA1; both intended Commons titles now held that SHA1.
  Invariant satisfied. The 2021 human redirect was exactly the
  corollary-2 case ("stale curatorial judgment about a partner-decided
  fact") that the invariant does not defer to. The apparent-bug was
  corollary 1 + corollary 2 — faithful projection plus a redirect
  that no longer bound the current sync.

Both incidents share the same trap for the reader: a "duplicate" that
LOOKS wrong because Commons's own dedup conventions were violated. In
both cases, the bot did exactly what the invariant requires.

## Where the invariant is enforced in code

`get_page_title` (in `ingest_wikimedia/wikimedia.py`) is the function
that defines "the intended title" for a DPLA ID. Its output is what
the invariant is defined against.

`process_file` (in `tools/uploader.py`) is the per-ordinal entry
point. Every branch of `process_file` either enforces the invariant
directly (uploads bytes to the intended title) or delegates to a
sub-function (`_resolve_hash_drift`, `_resolve_redirect_overwrite`,
`_resolve_redirect_move`) whose docstring names the invariant slice
it maintains.

`_resolve_hash_drift` dispatches to four named outcomes, all of which
are chosen because they either satisfy the invariant already or drive
the caller to a step that will. See the function's docstring for the
per-outcome mapping.

The docstrings on those functions link back to this document. If you
edit them, keep the link intact.
