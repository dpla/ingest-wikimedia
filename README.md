
# end-to-end process

There are three steps for uploading images to Wikimedia.

- `ingestion3` exports data for Wikimedia eligible records [see ingestion3 documentation](https://github.com/dpla/ingestion3#wikimedia)
- `ingest-wikimedia` download images from providers (staged in s3)
- `ingest-wikimedia` upload images from s3 to Wikimedia Commons

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

```shell
python3 wikimedia/run.py --input s3://dpla-master-dataset/ --output s3://dpla-wikimedia/ --partner ohio --type download
```

The download step expects four parameters

`--input` Root path of wikimedia exports from ingestion3 (ex. s3://dpla-master-dataset/)

`--output` Root path of where to save the images and data (ex. s3://dpla-wikimedia/)

`--partner` Name of the DPLA partner (ex. Ohio). Will be used as a prefix for both `--input` and `--output`

`--type` The type of event (ex. download)

```shell
python3 wikimedia/run.py \
--input s3://dpla-master-dataset/ \
--output s3://dpla-wikimedia/ \
--partner ohio \
--type download
```

`--limit` is an optionsal parameter and if omitted it will download all assests. This parameter is useful if a provider has multiple terrabytes of images and you don't want to download all of them in a single session (e.g. NARA or Texas).

## Running upload

Starting a new screen session for the uploads is helpful if you uploading multiple batches concurrently.

```shell
> screen -S il-upload-1
```

The invocation is very similar to the download

```shell
python3 wikimedia/run.py \
--input s3://dpla-wikimedia/ \
--partner ohio \
--type upload
```

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