
# Roadmap

Projects and improvements needed for this project

**[Wikimedia Workflow Google Doc](https://docs.google.com/document/d/1gkKjgdxy9zxP233DTxp_dZUw6WmOd05iEk9f7g0Q0Cc/edit?usp=sharing)**

## Logs

- [x] Log files are written to s3
- [ ] Log files should be emailed to tech@dp.la
- [ ] Create a `_SUMMARY` files similar to ones producted by ingestion3

## Downloads

- [x] Implement parsing of IIIF v3 manifests
  - Done in `ingest_wikimedia/iiif.py`: `get_iiif_urls` dispatches on the manifest `@context` to `iiif_v3_urls` (v3) or `iiif_v2_urls` (v2).
- [ ] Paralleize downloads
  - Paralleize around DPLA records (10 threads for 10 records).
  - For multi-page records the images are processed sequenctially. This might make aborting an entire record when one image fails.
  - *Needs further investigation*.
  - **Note**: the downloader is still single-process — only the SDC-sync phase has been parallelized (see Metadata Sync below). This item remains open.

- [x] Implement filter to skip an entire DPLA record if the record id exists in some kind of 'banned object list'.
  - Done: `ingest_wikimedia/banlist.py` (`Banlist`) reads `dpla-id-banlist.txt`; `tools/get_ids_es.py` calls `is_banned()` to drop banned IDs at ID-generation time (and pre-checks single-ID launches before the ES round-trip).
  - This is necessary to prevent our bot from getting banned when a page is deleted for copyright reasons and we automatiicaly re-attempt to upload it on the next cycle.

## Uploads

- [ ] **User logged out of Site**
  - In the pywikibot logs we can see that the user was logged out but the process still kept running.
  - There is likely a session timeout limit but it is not obvious what it is or how to trap that error and reauthenticate.
  - This may not have been an issue before because we ran smaller batches that finished before the session expired.

- [ ] **Existing pages**
  - *cases for that need addressings*

## Metadata Sync

A much larger unscropped set of work that is folded into the Wikimedia Workflow document

- [x] SDC-sync parallelism + box-wide worker-slot budget
  - SDC sync runs across N worker processes (`sdc-sync --workers N`, default 6 from the launcher/workflow).
  - A box-wide flock-backed slot budget (`--workers-budget N`, default 24) caps concurrent Commons writers across all sessions so they don't oversubscribe Commons' parser pool. See `ingest_wikimedia/worker_slots.py` and [operations → Worker-slot budget](docs/operations.md#worker-slot-budget).

## September 19th notes

> **Largely superseded.** The "updating existing asset" and "duplicate check" matrices below described hash-drift / duplicate handling that has since shipped: the downloader re-fetches on hash change, and the SDC-sync phase reconciles Commons against the precomputed `sdc.json` (and migrates legacy wikitext). Kept here for historical context; not an active work item.

- Dominic has deleted all the duplicates
- There are a number of items which may have misnumbered pages.
- Domiic to writ

Functionality for updating existing asset *(superseded — implemented as hash-drift handling + SDC sync)*

- Check hash on download; if different overwrite image in s3
- Upload asset will check if hash on s3 is different from hash in wiki
- overwirte if different

Duplicate check *(superseded)*
case 1: hash and filename match; no op
case 2: hash exists DPLA filename does not exist; move file to new page
case 3: hash exists, file name disagreement between Wiki and DPLA

- ignore duplicate warning
- force upload of hash collision to DPLA page
- continue for other other images in record

case 4: hash does not exist: upload to new page
case 5: hash does not exist page/file does exist; overwrite image in commons
