
# Roadmap

Projects and improvements needed for this project

**[Wikimedia Workflow Google Doc](https://docs.google.com/document/d/1gkKjgdxy9zxP233DTxp_dZUw6WmOd05iEk9f7g0Q0Cc/edit?usp=sharing)**

## Logs

- [x] Log files are written to s3
- [ ] Log files should be emailed to tech@dp.la
- [ ] Create a `_SUMMARY` files similar to ones producted by ingestion3

## Downloads

- [ ] Implement parsing of IIIF v3 manifests
- [x] Paralleize downloads
  - Paralleize around DPLA records (10 threads for 10 records).
  - For multi-page records the images are processed sequenctially. This might make aborting an entire record when one image fails.
  - *Needs further investigation*.

- [x] Implement filter to skip an entire DPLA record if the record id exists in some kind of 'banned object list'.

  - This is necessary to prevent our bot from getting banned when a page is deleted for copyright reasons and we automatiicaly re-attempt to upload it on the next cycle.
  - Likely implementation is a filter applied to data read in at Download step (similar to `--filter-filter`)

## Uploads

- [ ] **User logged out of Site**
  - In the pywikibot logs we can see that the user was logged out but the process still kept running.
  - There is likely a session timeout limit but it is not obvious what it is or how to trap that error and reauthenticate.
  - This may not have been an issue before because we ran smaller batches that finished before the session expired.

## Fixing the off-by-one bug with paginated records

All multi-page records uploaded since August(?) have this issue. Steps to remediate it include

- [ ] Dominic to delete all the duplicates
- [ ] Scott needs to implement all the cases under **Uploading images** section
- [ ] ...

## Uploading images

The cases that need to be implemented for correctly handling uploading images to Wikimedia Commons

### CASE 1

---
SHA1 hash of image on s3 and SHA1 hash of image in Commons match and filename match.

This is a no-op and should log a `Skipped` message

### CASE 2

---
The SHA1 of the iamge on s3 exists in Commons but the filename created by the Wikimedia ingest process does not exist on Commons. Move the file to a new page using pywikibot API. This will create a redirect from the old page name to the new page.

Log a `WARNING` message to indicate the the page title has changed.

### CASE 3

---
**TBD**  It is unclear to me right now what the difference between CASE 2 and CASE 3 based on the description below.

hash exists, file name disagreement between Wiki and DPLA

- ignore duplicate warning
- force upload of hash collision to DPLA page
- continue for other other images in record

### CASE 4

---
SHA1 hash does not exist in Commons and the page does not exist in Commons. Create new page and upload image. The is the most basic of cases.

Log an `INFO` message that a new image was uploaded.

### CASE 5

---
The SHA1 hash does not exist in Commons but the page does exist. Overwrite the image in Commons.

**TBD** Identify the API call to overwrite image.

Log an `INFO` message indicating an existing page was updated with new image.

## Structured Data on Commons + Metadata Sync

A much larger unscropped set of work that is folded into the Wikimedia Workflow document. The script exists in the project as `sdc-sync.py`.


## Miscellanious work

1. Analytics Dashboard work

   - Reported by Keila @ SSDN, she was trying to download a CSV of the Analytics data for the hub. This is an existing feature of the service that broke at some point in the past but percisely when is unclear.
   - Th work requested is not adding new functionality, just repair what broke.
   - This is bug in CSV where we are trying serialize a dict as a string and that type conversion errors out.
   - This is a medium / big lift and something I need to skill up around (I don't know Ruby or the AD code base at all)
