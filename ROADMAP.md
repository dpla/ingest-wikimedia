
# Roadmap

**[Wikimedia Workflow Google Doc](https://docs.google.com/document/d/1gkKjgdxy9zxP233DTxp_dZUw6WmOd05iEk9f7g0Q0Cc/edit?usp=sharing)**
Projects and improvements needed for this project

- Logging
- Downloads
- Uploads

## Logging

- [x] Log files are written to s3
- [x] Log files should be emailed to tech@dp.la
- [x] Create a `_SUMMARY` files similar to ones producted by ingestion3

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

## Logical flow

```python
# Evaluate CASE 1
if (FilePage(TITLE).exists() === TRUE):
  # evaluate CASE 5
  if (s3.SHA1 === commonsImage.SHA1):
    # Title and SHA1 match
    # 1. Log SKIPPED message
  else:
    # s3.SHA1 != commonsImage.SHA1
    # Does this SHA1 hash exist on Commons?
    if(s3.SHA1.exists() == TRUE):
      # 1. Implies that image is linked to another existing page in Commons
      # 2. Override IGNORE_WARNINGS array and remove IGNORE_DUPLICATE warning
      # 3. Upload image to Commons and replaces image on existing Commons page
      # 3. Log REPLACED message
      replaceImage()
    else:
      # Neither the SHA1 hash nor the Title exist on Commons
      # 1. Upload new image to commons
      # 2. Log UPLOAD message
      uploadNewImage()
else:
  # FilePage(TITLE).exists() === FALSE
  if(s3.SHA1.exists() == TRUE):
    # Title does not exist but the image has already been uploaded
    # Move the image to a new page
    # log MOVED message
    moveImage()
  else:
    # Neither the SHA1 hash nor the Title exist on Commons
    # Upload new image to commons
    # log UPLOAD message
    uploadNewImage

# page 1 does not exist
# image for page 1 exists but linked to page 2
#

def replaceImage()
# Replace image on existing page

def moveImage()
# Move existing image to new page

def uploadNewImage()
# Create new page

```

### CASE 1:  SHA1 Hash exists, matches Title

---
SHA1 hash of image on s3 and SHA1 hash of image in Commons match and filename match. This is one of the most basic cases and is a no-op. Logging should log a `Skipped` message.

TODO:

1. This case is already partially handled by the ingest script by checking the the page title on Commonss. It needs to account for the hash component as well.

NOTES:
> If hashes of images on s3 and hash of oringal images in institution repo are not checked then does the hash comparison even matter?
>
> I don't think it does and this should only check 'Title' for agreement.

### CASE 2: SHA1 exists, does not match Title

---
The SHA1 of the iamge on s3 exists in Commons but the filename created by the Wikimedia ingest process does not exist on Commons. Move the file to a new page using pywikibot API. This will create a redirect from the old page name to the new page.

Log a `WARNING` message to indicate the the page title has changed.

### CASE 3: SHA1 exists, ______

---
**TBD**  It is unclear to me right now what the difference between CASE 2 and CASE 3 based on the description below.

hash exists, file name disagreement between Wiki and DPLA

- ignore duplicate warning
- force upload of hash collision to DPLA page
- continue for other other images in record

### CASE 4: SHA1 does not exist, Title does not exist

---
SHA1 hash does not exist in Commons and the page does not exist in Commons. Create new page and upload image. The is the most basic of cases.

Log an `INFO` message that a new image was uploaded.

### CASE 5: SHA1 does not exist, Title does exist

---
The SHA1 hash does not exist in Commons but the page does exist. Overwrite the image in Commons.

**TBD** Identify the API call to overwrite image.

Log an `INFO` message indicating an existing page was updated with new image.

## Structured Data on Commons + Metadata Sync

A much larger unscropped set of work that is folded into the Wikimedia Workflow document. The script exists in the project as `sdc-sync.py`.
