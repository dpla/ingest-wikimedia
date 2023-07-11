
# end-to-end process

There are three high level steps for uploading image to Wikimedia

- ingestion3 Wikimedia export
- ingest-wikimedia download
- ingest-wikimedia upload

The first step when running ingests is to start the `wikimedia` ec2 instance

- log into AWS console
- go to the ec2 panel
- find stopped `wikimedia` instances
- change state from stopped to start

This box was created by Vagrant (see `Vagrantfile` in project) so you can ssh into the box by using `vagrant ssh`

Start a screen session so that the processes can continue after you log off the instance. It is also useful to set a screen session name.

## Running download

Running a download requires two pieces of information

1) The path to most recent Wikimedia parquet file export from ingestion3.
2) The path to save the output in s3

The most recent Wikimedia export from ingestion3 can be identified by using the AWS CLI.

```shell
> aws s3 ls s3://dpla-master-dataset/il/wiki/                                                                                                             
    PRE 20220719_182758-il-wiki.parquet/
    PRE 20221027_195109-il-wiki.parquet/
    PRE 20230130_201856-il-wiki.parquet/

```

We are going to download images from this set of eligible records identified during the ingest process

`s3://dpla-master-dataset/il/wiki/20230130_201856-il-wiki.parquet/`

The path to output would be (this is determined by *you* and does not need to confirm to any specific formatting but consistent naming is very useful)

`s3://dpla-wikimedia/il/20230130/`

With these two pieces we are now ready to kick off the download within the previously activated screen session.

```shell
> cd ~/ingest-wikimedia/ 
> source venv-3.10/bin/activate 
> poetry run python downloader.py --input s3://dpla-master-dataset/il/wiki/20230130_201856-il-wiki.parquet/ --output s3://dpla-wikimedia/il/20230130/ --batch_size 500000000000  --limit 5000000000000
```

The `--batch-size` and `--limit` can be adjusted as needed.

## Running upload

Generally, starting a new screen session for the upload is helpful if you uploading multiple batches concurrently so you can track the batch by session name

```shell
> :sessionname il-up-1
```

When a downloaded batch is completed then the upload for that batch can be executed. The invocation is very similar to the download

```shell
> cd ~/ingest-wikimedia/; 
> source venv-3.10/bin/activate; 
> poetry run python upload.py --input s3://dpla-wikimedia/il/20230130/batch_1/
```

When all the downloads and uploads for the month have been completed go back to the ec2 console and **stop** the `wikimedia` instance.

# Useful links

- <https://presentation-validator.iiif.io/i>