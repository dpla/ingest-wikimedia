
# end-to-end process

There are three steps for uploading images to Wikimedia.

- Export data for eligible records from the monthly ingest process [see ingestion3 documentation](https://github.com/dpla/ingestion3#wikimedia)
- ingest-wikimedia `run-download.py`
- ingest-wikimedia upload

## Starting up

Start the `wikimedia` ec2 instance

```shell
> ec2-start wikimedia
```

Creates a new screen session with a name of `nwdh` and attach to that session. Use `-xS` to reattach after disconnecting.

```shell
> screen -S nwdh
> screen -xS nwdh
```

[A quick cheat sheet](https://gist.github.com/jctosta/af918e1618682638aa82) for `screen` commands.

## Running download

Running a download requires two pieces of information

1) The path to the Wikimedia output from ingestion3
2) The path to save the output in s3

The inpout path should be standard

`s3://dpla-master-dataset/il/wiki/`

The path to output would be (this is determined by *you* and does not need to confirm to any specific formatting but consistent naming is very useful)

`s3://dpla-wikimedia/il/`

With these two pieces we are now ready to kick off the download within the previously activated screen session.

```shell
> cd ~/ingest-wikimedia/
> source venv-3.10/bin/activate
> poetry run python run-download.py \
    --input s3://dpla-master-dataset/il/wiki/ \
    --output s3://dpla-wikimedia/il/ \
    --partner il
```

`--limit` is an optionsal parameter and if omitted it will download all assests. This parameter is useful if a provider has multiple terrabytes of images and you don't want to download all of them in a single session (e.g. NARA or Texas).

## Running upload

Starting a new screen session for the uploads is helpful if you uploading multiple batches concurrently.

```shell
> screen -S il-upload-1
```

The invocation is very similar to the download

```shell
> cd ~/ingest-wikimedia/;
> source venv-3.10/bin/activate;
> poetry run python upload-entry.py \
--input s3://dpla-wikimedia/il/
--partner il
```

## Logs

Log files are written out on the local file system in `./ingest-wikimedia/logs/` and on sucessful completetion written to s3 `s3://dpla-wikimedia/il/logs/`).

## Closing out

When all the downloads and uploads for the month have been completed stop the `wikimedia` instance.

```shell
> ec2-stop wikimedia
```

# Useful links

- [IIIF validator](https://presentation-validator.iiif.io/i)
- [Cheat sheet](https://gist.github.com/jctosta/af918e1618682638aa82) for `screen` commands