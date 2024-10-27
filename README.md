# ingest-wikimedia

downloader.py [OPTIONS] IDS_FILE PARTNER API_KEY
uploader.py [OPTIONS] IDS_FILE PARTNER API_KEY

## Logs

Log files are written out on the local file system in `./ingest-wikimedia/logs/` and on sucessful completetion written to s3 `s3://dpla-wikimedia/ohio/logs/`).

## Closing out

When all the downloads and uploads for the month have been completed stop the `wikimedia` instance.

```shell
> ec2-stop wikimedia
```

# Useful links

- [IIIF validator](https://presentation-validator.iiif.io/i)
- [Cheat sheet](https://gist.github.com/jctosta/af918e1618682638aa82) for `screen` commands
