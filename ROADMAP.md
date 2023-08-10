
# Roadmap
Projects and improvements needed for this project

**[Wikimedia Workflow Google Doc](https://docs.google.com/document/d/1gkKjgdxy9zxP233DTxp_dZUw6WmOd05iEk9f7g0Q0Cc/edit?usp=sharing)**

## Logs

- [x] Log files are written to s3
- [ ] Log files should be emailed to tech@dp.la
- [ ] Create a `_SUMMARY` files similar to ones producted by ingestion3

## Downloads

- [ ] **Paralleize downloads**. I think the best approach here is to paralleize around each DPLA record. So we have have 10 threads for 10 records. For the multipage records the images are processed sequenctially. This might make aborting an entire record when one image fails. *Needs further investigation*.
- [ ] Implement parsing of IIIF v3 manifests 

## Uploads 
- [ ] **User logged out of Site()** Saw this in the pywikibot logs and not sure what triggered the logout. Probably some time limit but I'm not sure how we can trap that error and reauthenticate. May not have been an issue before because we ran smaller batches that finished before the session expired.

- [ ] **Existing pages** 
  - *cases for that need addressings*
  

## Metadata Sync
A much larger unscropped set of work that is folded into the Wikimedia Workflow document 