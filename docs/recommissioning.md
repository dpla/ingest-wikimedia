{% raw %}
# Pause State and Recommissioning

The Wikimedia upload work was paused in November 2026. This document records what was shut down, where everything needed to restart is kept, and the exact steps to bring up a replacement EC2 box and resume runs.

Read [Pause log](#pause-log) first: it says which parts of the system are currently off. Then follow [Path A](#path-a-restore-the-box-from-the-snapshot) if the snapshot still exists, or [Path B](#path-b-build-a-box-from-scratch) if it does not, and finish with [Check the box](#check-the-box) and [Repoint and re-enable](#repoint-and-re-enable).

## Table of contents

1. [Pause log](#pause-log)
2. [What the pipeline needs to run](#what-the-pipeline-needs-to-run)
3. [Where the saved state lives](#where-the-saved-state-lives)
4. [Secrets](#secrets)
5. [Path A: restore the box from the snapshot](#path-a-restore-the-box-from-the-snapshot)
6. [Path B: build a box from scratch](#path-b-build-a-box-from-scratch)
8. [Check the box](#check-the-box)
9. [Repoint and re-enable](#repoint-and-re-enable)
10. [S3 state and what it means for the first runs](#s3-state-and-what-it-means-for-the-first-runs)
11. [Smoke test](#smoke-test)

---

## Pause log

Update this table whenever a piece of the system is turned off or back on. It is the only place that records the current state.

| Date | Action | How to reverse |
|---|---|---|
| 2026-09-30 | Scheduled CIM pageviews runs paused (cron block commented out in `cim-pageviews.yml`, PR #435) | Uncomment the `schedule:` block |
| 2026-11-18 | Last running session (`wikimedia-bpl`, a download started 2026-07-17) killed via `wikimedia-kill.yml` | n/a |
| 2026-11-18 | `wikimedia-upload-status.yml` disabled in GitHub (stops the 6-hourly cron and `/wikimedia-status`) | `gh workflow enable wikimedia-upload-status.yml --repo dpla/ingest-wikimedia` |
| 2026-11-18 | Box logs, working files and home directory backed up to S3 (see [Where the saved state lives](#where-the-saved-state-lives)) | n/a |
| 2026-11-18 | EBS snapshot `snap-0a1b2f9178032b106` taken of the box's root volume | n/a |

To check the live state rather than trust this table:

```bash
aws ec2 describe-instances --region us-east-1 --instance-ids i-033eff6c8c168f999 \
  --query 'Reservations[].Instances[].State.Name' --output text      # errors or "terminated" if the box is gone
gh workflow list --repo dpla/ingest-wikimedia --all                  # which workflows are disabled
aws s3 ls s3://dpla-wikimedia/                                       # which partner prefixes remain
```

## What the pipeline needs to run

| Piece | Value at the time of the pause |
|---|---|
| Instance | `i-033eff6c8c168f999`, Name tag `wiki downloads`, `m7g.large` (arm64, 2 vCPU, ~7.6 GiB RAM, no swap) |
| OS | Amazon Linux 2023, launched from `ami-0013610ea966aafe0` (`al2023-ami-2023.6.20250317.2-kernel-6.1-arm64`), UEFI boot |
| Root volume | 100 GB gp3 (3000 IOPS, 125 MB/s), unencrypted, device `/dev/xvda`; about 36 GB used |
| Network | VPC `vpc-b36ab3d6`, subnet `subnet-e8c8f3c0` (`172.30.0.0/24`, us-east-1a, auto-assigns a public IP), security group `sg-58f1803d` (`default`) |
| Why that subnet | The box must reach Elasticsearch at `search-prod1.internal.dp.la:9200` (`ingest_wikimedia/es.py`), which is only reachable inside the VPC |
| Instance profile | `ingestion3-spark` (gives `s3:*` on `dpla-wikimedia`); shared with the ingest box, so never delete it |
| SSM | Works through the account's Default Host Management role; no extra instance policy needed |
| Key pair | `2025` |
| Metadata options | IMDSv2 required, hop limit 2 |
| Public IP | Auto-assigned, not an Elastic IP. No partner allowlists this box's address |
| User | Everything runs as `ec2-user` from `/home/ec2-user/ingest-wikimedia/` (hardcoded as `INGEST_WIKI_ROOT` in `ingest_wikimedia/partners.py`) |
| Python | 3.13 managed by `uv` (`/home/ec2-user/.local/bin/uv`); venv at `/home/ec2-user/ingest-wikimedia/.venv` |
| System packages | `tmux`, `git`, plus the build packages in the launch script below. `python-magic` uses `file-libs`, which is in the base image |

**The instance ID is hardcoded.** A replacement box gets a new ID, so every item in [Repoint and re-enable](#repoint-and-re-enable) is mandatory after either path.

**Run commands as `ec2-user`, not root.** Over SSM, commands run as root, and root's AWS identity is the SSM management role, which cannot write to S3. `sudo -u ec2-user` picks up the `ingestion3-spark` instance profile. The launcher already does this.

## Where the saved state lives

### EBS snapshot

`snap-0a1b2f9178032b106` is a snapshot of the whole root volume as it stood on 2026-11-18, with the box idle. It contains the full working install, including the secrets listed below. It is unencrypted, as the volume was.

### S3 backup

`s3://dpla-wikimedia/_box-backup/i-033eff6c8c168f999-20261118/`

| Object | Contents |
|---|---|
| `logs-<partner>.tar.gz` | That partner's `logs/` directory. `logs-_toplevel.tar.gz` is `ingest-wikimedia/logs/` |
| `workdir.tar.gz` | `ingest-wikimedia/` without logs, the venv, temp directories or secrets: ID lists (`*.csv`), `hand-fix.jsonl`, loose scripts, the deployed code |
| `home.tar.gz` | Everything else in `/home/ec2-user` except dotfiles: `dupscan/`, `nara-audit/`, `drift_check/`, `dup-recovery/`, `drift-remediation/`, `nomrs-purge/`, one-off scripts and their output |
| `user-data.sh` | The instance's original launch script |
| `REBUILD-NOTES.txt` | OS version, tool versions, package list, and the list of files deliberately left out |
| `FILE-LIST.txt.gz` | Every path in the archives |

The backup deliberately contains **no secrets**. It also skips anything regenerable: the venv, `~/.cache`, `wiki-tmp-*` download temp directories, pywikibot `apicache/` and `throttle.ctrl`, system logs.

If the bucket is ever cleaned up, keep the `_box-backup/` prefix along with the sidecars.

## Secrets

These exist on the box's disk (and therefore in the snapshot) and are **not** in the S3 backup.

| File on the box | What it holds | Where else it exists |
|---|---|---|
| `ingest-wikimedia/config.toml` | `dpla_api_key`, and a `[dpla_secrets]` table with one entry per hub (13 hubs) | Possibly nowhere else. Confirm before deleting the snapshot |
| `ingest-wikimedia/user-password.py` | Bot password for `DPLA bot`, bot-password name `PARTNER_UPLOADS` | GitHub secret `PYWIKIBOT_PASSWORD` (write-only). Can be regenerated at `Special:BotPasswords` on Commons |
| `~/.bashrc` | `export DPLA_SLACK_BOT_TOKEN=…` | GitHub secret `DPLA_SLACK_BOT_TOKEN` (write-only). Can be reissued from the Slack app |
| `<partner>/pywikibot-DPLA_bot.lwp` | Login cookies | Regenerated on first login |

Off the box:

- **Lambda `wikimedia-slack-dispatch`** holds `SLACK_SIGNING_SECRET` and `GH_TOKEN`. `GH_TOKEN` is a fine-grained GitHub token with `actions:write` on this repo; fine-grained tokens expire, so check it first if slash commands fail after a long pause.
- **GitHub Actions** authenticates to AWS through OIDC (role `github-actions-ingest-wikimedia`); see [github-actions-aws-oidc.md](github-actions-aws-oidc.md). No stored AWS keys are needed.

File layout that the code expects:

- `config.toml` is read from the **current working directory** by the phase tools (`ingest_wikimedia/tools_context.py`) and from the repo root by `sdc-sync`. Each partner directory therefore needs its own `config.toml`, `user-config.py` and `user-password.py`. On the old box these were symlinks to the copies in the repo root.
- `user-config.py` is not secret. Its contents:

  ```python
  password_file = "/home/ec2-user/ingest-wikimedia/user-password.py"
  console_encoding = "utf-8"
  put_throttle = 1
  # Production
  usernames["commons"]["commons"] = "DPLA bot"
  mylang = "commons"
  family = "commons"
  usernames["wikidata"]["wikidata"] = "DPLA bot"
  ```

- `user-password.py` is one line: `("DPLA bot", BotPassword("PARTNER_UPLOADS", "<password>"))`.
- The launcher creates a missing partner directory by copying `config.toml`, `user-config.py` and `user-password.py` from the **`bpl/`** directory, and refuses to launch if `bpl/` does not exist (`scripts/wikimedia_launch.py`). So `bpl/` must exist and hold those three files.
- The pipeline script runs `source ~/.bashrc` before each session, which is how the Slack token reaches the phase tools.

## Path A: restore the box from the snapshot

This is the fast path: the result is the old box exactly, secrets and logs included. **These commands were written at the pause and have not been exercised.** The image attributes mirror the original AMI (arm64, UEFI, ENA, IMDSv2).

```bash
export AWS_DEFAULT_REGION=us-east-1
SNAP=snap-0a1b2f9178032b106

# 1. Register a launchable image from the snapshot.
AMI=$(aws ec2 register-image \
  --name "wiki-downloads-restore-$(date +%Y%m%d)" \
  --architecture arm64 --virtualization-type hvm --boot-mode uefi \
  --ena-support --imds-support v2.0 \
  --root-device-name /dev/xvda \
  --block-device-mappings "DeviceName=/dev/xvda,Ebs={SnapshotId=$SNAP,VolumeSize=100,VolumeType=gp3,DeleteOnTermination=true}" \
  --query ImageId --output text)

# 2. Launch it with the same placement and role as the old box.
aws ec2 run-instances \
  --image-id "$AMI" --instance-type m7g.large --key-name 2025 \
  --subnet-id subnet-e8c8f3c0 --security-group-ids sg-58f1803d \
  --iam-instance-profile Name=ingestion3-spark \
  --metadata-options "HttpTokens=required,HttpPutResponseHopLimit=2,HttpEndpoint=enabled" \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=wiki downloads}]' \
  --query 'Instances[0].InstanceId' --output text
```

Wait for the instance to appear in `aws ssm describe-instance-information`, then continue at [Check the box](#check-the-box).

Notes:

- The restored disk has a 2025 OS and a nightly `netdata` build. Run `sudo dnf upgrade -y` once it is up.
- The restored disk still contains 338 stale `wiki-tmp-*` directories. Clear them: `find /home/ec2-user/ingest-wikimedia -maxdepth 2 -name 'wiki-tmp-*' -exec rm -rf {} +`

## Path B: build a box from scratch

Use this if the snapshot is gone. You will need the three secrets from [Secrets](#secrets) from another source.

### 1. Launch

Save the package-install line from the original launch script as `user-data.sh`. (The full original is at `_box-backup/…/user-data.sh`; its other lines installed `uv` for root and `s5cmd`, neither of which the pipeline uses.)

```bash
#!/bin/bash
sudo dnf install tmux git emacs-nox gcc make patch zlib-devel bzip2 bzip2-devel readline-devel sqlite sqlite-devel openssl-devel tk-devel libffi-devel xz-devel -y
```

Then:

```bash
export AWS_DEFAULT_REGION=us-east-1
AMI=$(aws ssm get-parameter \
  --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64 \
  --query Parameter.Value --output text)

aws ec2 run-instances \
  --image-id "$AMI" --instance-type m7g.large --key-name 2025 \
  --subnet-id subnet-e8c8f3c0 --security-group-ids sg-58f1803d \
  --iam-instance-profile Name=ingestion3-spark \
  --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=100,VolumeType=gp3,DeleteOnTermination=true}' \
  --metadata-options "HttpTokens=required,HttpPutResponseHopLimit=2,HttpEndpoint=enabled" \
  --user-data file://user-data.sh \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=wiki downloads}]' \
  --query 'Instances[0].InstanceId' --output text
```

Size note: `m7g.large` allows only one large-media (NARA-class) run at a time; see [operations.md → Memory and concurrency planning](operations.md#memory-and-concurrency-planning). Pick a larger type here if that was a constraint.

### 2. Install the code (as `ec2-user`)

The original launch script installed `uv` as root, which is why it is repeated here for the right user.

```bash
sudo -iu ec2-user
curl -LsSf https://astral.sh/uv/install.sh | sh          # installs ~/.local/bin/uv
git clone https://github.com/dpla/ingest-wikimedia.git ~/ingest-wikimedia
cd ~/ingest-wikimedia
~/.local/bin/uv sync                                      # creates .venv with Python 3.13 and the CLI entry points
ls .venv/bin/{get-ids-es,downloader,uploader,sdc-sync}    # confirm
```

The launcher redeploys code on every run by cloning to `/tmp` and copying `ingest_wikimedia/`, `tools/`, `pyproject.toml` and `uv.lock` over this directory, then running `uv sync`. It does not need the directory to be a git checkout.

### 3. Put the secrets and bot config in place

```bash
cd ~/ingest-wikimedia
# config.toml: dpla_api_key = "…" and a [dpla_secrets] table, one key per hub slug
# user-config.py and user-password.py: contents shown in the Secrets section
chmod 600 user-password.py config.toml

mkdir -p bpl
for f in config.toml user-config.py user-password.py; do ln -s ../$f bpl/$f; done

echo 'export DPLA_SLACK_BOT_TOKEN=<token>' >> ~/.bashrc
```

Other partner directories are created by the launcher from `bpl/` on first use.

### 4. Restore working files (optional)

Only `hand-fix.jsonl` and the logs carry information that cannot be regenerated; the retry tool reads logs, but only recent ones.

```bash
B=s3://dpla-wikimedia/_box-backup/i-033eff6c8c168f999-20261118
aws s3 cp $B/workdir.tar.gz - | tar -xzf - -C /home/ec2-user --skip-old-files
aws s3 cp $B/logs-nara.tar.gz - | tar -xzf - -C /home/ec2-user/ingest-wikimedia/nara     # per partner, as needed
```

`--skip-old-files` keeps the fresh checkout's code where the archive has an older copy.

## Check the box

After either path, as `ec2-user` on the new box:

```bash
curl -s -m 5 -o /dev/null -w '%{http_code}\n' http://search-prod1.internal.dp.la:9200/   # expect 200
aws sts get-caller-identity --query Arn --output text                                    # expect …assumed-role/ingestion3-spark/…
aws s3 ls s3://dpla-wikimedia/ | head -3
cd ~/ingest-wikimedia/bpl && source ../.venv/bin/activate && python -c "import pywikibot; s=pywikibot.Site(); s.login(); print(s.user())"   # expect DPLA bot
```

## Repoint and re-enable

`NEW` is the new instance ID.

1. **Code.** Change `INSTANCE_ID` in `ingest_wikimedia/ssm.py` and merge to `main`. The workflows only run from `main`.
2. **IAM, GitHub Actions role.** The inline policy `wikimedia-ssm` on role `github-actions-ingest-wikimedia` allows `ssm:SendCommand` only to the old instance ARN. Replace the instance ARN with `arn:aws:ec2:us-east-1:283408157088:instance/NEW`:

   ```bash
   aws iam get-role-policy --role-name github-actions-ingest-wikimedia --policy-name wikimedia-ssm \
     --query PolicyDocument > wikimedia-ssm.json
   # edit the instance ARN in wikimedia-ssm.json, then:
   aws iam put-role-policy --role-name github-actions-ingest-wikimedia --policy-name wikimedia-ssm \
     --policy-document file://wikimedia-ssm.json
   ```

   The managed policy `wikimedia-status-ssm` (attached to the legacy IAM user `wikimedia-status-gh-action`) also names the old instance. It predates OIDC and is unused; delete the user and policy rather than update them.
3. **Docs.** Replace the old instance ID in `docs/architecture.md`, `docs/github-actions-aws-oidc.md`, `docs/maintenance-tools.md`, `docs/operations.md` and this file.
4. **Workflows.** Reverse each row of the [Pause log](#pause-log) that turned something off, using the command in its last column.
5. **Lambda.** Nothing to change unless slash commands fail: then check that `GH_TOKEN` has not expired, and redeploy the handler if `lambda/wikimedia-slack-dispatch/handler.py` changed during the pause ([operations.md → Deploying the Lambda](operations.md#deploying-the-lambda)).

## S3 state and what it means for the first runs

`s3://dpla-wikimedia/<partner>/images/…` holds two kinds of object per item (see [sidecars.md](sidecars.md)): the media bytes (`<ordinal>_<dpla-id>`) and small sidecars (`dpla-map.json`, `sdc.json`, `file-list.txt`, `upload-result.json`, `iiif.json`). At the pause the bucket held about 120 TB, almost all of it media.

If the media bytes have been deleted to save storage and only sidecars kept (check the [Pause log](#pause-log)):

- **Still works without re-downloading:** `/wikimedia-upload sdc …` and `/wikimedia-upload maintain lite …`. Both run from sidecars and the live Commons state.
- **Needs media again:** a normal upload run and the default `maintain` route. The uploader reads each file from S3 even when the result is a skip, so the downloader will re-fetch everything in scope from the partner first. Plan runs institution by institution rather than whole hubs.
- **SHA1s:** the `CHECKSUM` metadata lived on the media objects, so it is gone with them. Commons still has each file's SHA1, and the downloader re-stamps it on re-download.

If sidecars were deleted too, `upload-result.json` (the map from DPLA item and ordinal to Commons title and page ID) is rebuilt by a normal upload run, and `sdc-sync` can recover existing files by DPLA ID search in the meantime ([sdc-sync.md → Eligibility, ordinal rescue & pageid self-heal](sdc-sync.md#eligibility-ordinal-rescue--pageid-self-heal)).
## Smoke test

In order, cheapest first:

1. `/wikimedia-status` in Slack. Expect `No active Wikimedia upload sessions.` plus a memory line in #tech-alerts. This proves Slack → Lambda → GitHub Actions → OIDC → SSM → box.
2. `/wikimedia-upload maintain count "<hub>|<institution>"` for a small institution. Read-only: it exercises Elasticsearch, S3 and the Commons login without writing anything.
3. `/wikimedia-upload <dpla-id>` for one item known to be on Commons already. A healthy result is a skip in the upload phase and zero SDC writes. If media was deleted from S3, this re-downloads that one item first.

{% endraw %}
