
# Roadmap

Projects and improvements needed for this project

**[Wikimedia Workflow Google Doc](https://docs.google.com/document/d/1gkKjgdxy9zxP233DTxp_dZUw6WmOd05iEk9f7g0Q0Cc/edit?usp=sharing)**

## Logs

- [x] Log files are written to s3
- [ ] Log files should be emailed to tech@dp.la
- [ ] Create a `_SUMMARY` files similar to ones producted by ingestion3

## Downloads

- [ ] **Paralleize downloads**. 
  - Paralleize around DPLA records (10 threads for 10 records). 
  - For multi-page records the images are processed sequenctially. This might make aborting an entire record when one image fails.
  - *Needs further investigation*.
- [ ] Implement parsing of IIIF v3 manifests

## Uploads 

- [ ] **User logged out of Site** 
  - In the pywikibot logs we can see that the user was logged out but the process still kept running.
  - There is likely a session timeout limit but it is not obvious what it is or how to trap that error and reauthenticate.
  - This may not have been an issue before because we ran smaller batches that finished before the session expired.

- [ ] **Existing pages**
  - *cases for that need addressings*
  
## Metadata Sync

A much larger unscropped set of work that is folded into the Wikimedia Workflow document $$