import json
from io import BytesIO
from unittest.mock import MagicMock, call, patch

import pytest

from ingest_wikimedia.common import CHECKSUM
from ingest_wikimedia.dpla import IIIF_MANIFEST_FIELD_NAME, MEDIA_MASTER_FIELD_NAME
from ingest_wikimedia.tracker import Result


@pytest.fixture
def downloader():
    from tools.downloader import Downloader

    web = MagicMock()
    web.get_http_session.return_value = MagicMock()

    return Downloader(
        provider="test_provider",
        tracker=MagicMock(),
        s3_client=MagicMock(),
        web=web,
        local_fs=MagicMock(),
        iiif=MagicMock(),
    )


# ---------------------------------------------------------------------------
# Fix 1: zero-byte stub bypass in upload_file_to_s3
# ---------------------------------------------------------------------------


def _make_s3_obj(sha1: str, content_length):
    obj = MagicMock()
    obj.metadata = {CHECKSUM: sha1}
    obj.content_length = content_length
    return obj


def test_upload_skips_when_checksum_matches_and_nonzero(downloader):
    sha1 = "abc123"
    obj = _make_s3_obj(sha1, 1024)

    mock_s3 = MagicMock()
    mock_s3.Object.return_value = obj
    downloader.s3_client.get_s3.return_value = mock_s3

    with patch(
        "builtins.open",
        return_value=MagicMock(
            __enter__=lambda s: BytesIO(b"data"), __exit__=MagicMock(return_value=False)
        ),
    ):
        with patch("os.stat") as mock_stat:
            mock_stat.return_value.st_size = 1024
            downloader.upload_file_to_s3("/tmp/fake", "dest/path", "image/jpeg", sha1)

    downloader.tracker.increment.assert_called_once_with(Result.SKIPPED)
    obj.upload_fileobj.assert_not_called()


def test_upload_proceeds_when_checksum_matches_but_zero_bytes(downloader):
    sha1 = "abc123"
    obj = _make_s3_obj(sha1, 0)

    mock_s3 = MagicMock()
    mock_s3.Object.return_value = obj
    downloader.s3_client.get_s3.return_value = mock_s3

    fake_file = BytesIO(b"real content")
    fake_file.name = "/tmp/fake"

    with patch("builtins.open") as mock_open:
        mock_open.return_value.__enter__ = lambda s: fake_file
        mock_open.return_value.__exit__ = MagicMock(return_value=False)
        with patch("os.stat") as mock_stat:
            mock_stat.return_value.st_size = len(b"real content")
            downloader.upload_file_to_s3("/tmp/fake", "dest/path", "image/jpeg", sha1)

    for c in downloader.tracker.increment.call_args_list:
        assert c != call(Result.SKIPPED), "Should not skip a zero-byte stub"
    obj.upload_fileobj.assert_called_once()


def test_upload_force_overwrite_touches_via_copy_when_sha1_matches(downloader):
    """Refresh / overwrite path: when the freshly-downloaded SHA1 matches
    the existing S3 object, we must update LastModified via copy-self
    instead of skipping. Skipping was the bug that made `--max-age-days`
    refreshes run forever — every periodic refresh would log "Refreshing
    X: file is 554 days old", actually re-download the bytes, see the
    matching SHA1, skip the upload, and leave the S3 LastModified at the
    original write, so next run still saw the file as 554+ days old.
    """
    sha1 = "abc123"
    existing_metadata = {CHECKSUM: sha1, "custom": "preserved"}
    obj = MagicMock()
    obj.metadata = existing_metadata
    obj.content_length = 1024

    mock_s3 = MagicMock()
    mock_s3.Object.return_value = obj
    downloader.s3_client.get_s3.return_value = mock_s3

    with patch(
        "builtins.open",
        return_value=MagicMock(
            __enter__=lambda s: BytesIO(b"data"), __exit__=MagicMock(return_value=False)
        ),
    ):
        with patch("os.stat") as mock_stat:
            mock_stat.return_value.st_size = 1024
            result = downloader.upload_file_to_s3(
                "/tmp/fake",
                "dest/path",
                "image/jpeg",
                sha1,
                force_overwrite=True,
            )

    assert result is True
    # copy_object must be called to touch LastModified
    mock_s3.meta.client.copy_object.assert_called_once()
    kwargs = mock_s3.meta.client.copy_object.call_args.kwargs
    assert kwargs["MetadataDirective"] == "REPLACE"
    # MetadataDirective REPLACE drops every header not re-supplied — the full
    # existing metadata (including CHECKSUM and any custom keys) must be
    # passed back in. See lessons.md "AWS S3 copy_object with
    # MetadataDirective".
    assert kwargs["Metadata"][CHECKSUM] == sha1
    assert kwargs["Metadata"]["custom"] == "preserved"
    # Body re-upload must NOT happen — bytes are identical.
    obj.upload_fileobj.assert_not_called()
    downloader.tracker.increment.assert_called_once_with(Result.DOWNLOADED)


def test_upload_force_overwrite_uploads_when_sha1_differs(downloader):
    """When force_overwrite=True and the new SHA1 differs from the existing
    S3 object's SHA1, the standard upload path runs (no SHA1-match guard
    to begin with, so behavior here matches the default case)."""
    obj = _make_s3_obj("old_sha", 1024)

    mock_s3 = MagicMock()
    mock_s3.Object.return_value = obj
    downloader.s3_client.get_s3.return_value = mock_s3

    fake_file = BytesIO(b"new content")
    fake_file.name = "/tmp/fake"

    with patch("builtins.open") as mock_open:
        mock_open.return_value.__enter__ = lambda s: fake_file
        mock_open.return_value.__exit__ = MagicMock(return_value=False)
        with patch("os.stat") as mock_stat:
            mock_stat.return_value.st_size = len(b"new content")
            result = downloader.upload_file_to_s3(
                "/tmp/fake",
                "dest/path",
                "image/jpeg",
                "new_sha",
                force_overwrite=True,
            )

    assert result is True
    obj.upload_fileobj.assert_called_once()
    mock_s3.meta.client.copy_object.assert_not_called()


def test_upload_default_skips_when_sha1_matches(downloader):
    """Existing behavior preserved: without force_overwrite, a matching
    SHA1 short-circuits the upload (no copy-self, no upload)."""
    sha1 = "abc123"
    obj = _make_s3_obj(sha1, 1024)

    mock_s3 = MagicMock()
    mock_s3.Object.return_value = obj
    downloader.s3_client.get_s3.return_value = mock_s3

    with patch(
        "builtins.open",
        return_value=MagicMock(
            __enter__=lambda s: BytesIO(b"data"), __exit__=MagicMock(return_value=False)
        ),
    ):
        with patch("os.stat") as mock_stat:
            mock_stat.return_value.st_size = 1024
            result = downloader.upload_file_to_s3(
                "/tmp/fake", "dest/path", "image/jpeg", sha1
            )

    assert result is False
    obj.upload_fileobj.assert_not_called()
    mock_s3.meta.client.copy_object.assert_not_called()
    downloader.tracker.increment.assert_called_once_with(Result.SKIPPED)


def test_upload_proceeds_when_content_length_unreadable(downloader):
    sha1 = "abc123"
    obj = _make_s3_obj(sha1, None)  # None → TypeError on int()

    mock_s3 = MagicMock()
    mock_s3.Object.return_value = obj
    downloader.s3_client.get_s3.return_value = mock_s3

    fake_file = BytesIO(b"data")
    fake_file.name = "/tmp/fake"

    with patch("builtins.open") as mock_open:
        mock_open.return_value.__enter__ = lambda s: fake_file
        mock_open.return_value.__exit__ = MagicMock(return_value=False)
        with patch("os.stat") as mock_stat:
            mock_stat.return_value.st_size = 4
            downloader.upload_file_to_s3("/tmp/fake", "dest/path", "image/jpeg", sha1)

    for c in downloader.tracker.increment.call_args_list:
        assert c != call(Result.SKIPPED)
    obj.upload_fileobj.assert_called_once()


def test_upload_refuses_zero_byte_local_file_no_existing(downloader):
    # Defense in depth: even if download_file_to_temp_path's raise-on-empty
    # check were to be bypassed, upload_file_to_s3 must not write a 0-byte
    # object to S3. This is the bug that created stubs in the first place.
    from botocore.exceptions import ClientError

    mock_s3 = MagicMock()
    # Existing object doesn't exist → 404 when .metadata is read
    obj = MagicMock()
    type(obj).metadata = property(
        lambda _: (_ for _ in ()).throw(
            ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
            )
        )
    )
    mock_s3.Object.return_value = obj
    downloader.s3_client.get_s3.return_value = mock_s3

    with patch("os.stat") as mock_stat:
        mock_stat.return_value.st_size = 0  # 0-byte local file
        result = downloader.upload_file_to_s3(
            "/tmp/empty", "dest/path", "image/jpeg", "abc123"
        )

    assert result is False
    obj.upload_fileobj.assert_not_called()
    downloader.tracker.increment.assert_any_call(Result.FAILED)
    # Lock in "at the source" — guard fires before any S3 lookup.
    downloader.s3_client.get_s3.assert_not_called()


def test_upload_refuses_zero_byte_local_file_over_existing_valid(downloader):
    # Existing S3 has valid content; new download is 0 bytes — must not
    # overwrite, and must not silently SKIP either (FAILED is the honest
    # signal — the download produced no usable content).
    obj = _make_s3_obj("existing_sha", 1024)
    mock_s3 = MagicMock()
    mock_s3.Object.return_value = obj
    downloader.s3_client.get_s3.return_value = mock_s3

    with patch("os.stat") as mock_stat:
        mock_stat.return_value.st_size = 0
        result = downloader.upload_file_to_s3(
            "/tmp/empty", "dest/path", "image/jpeg", "new_sha"
        )

    assert result is False
    obj.upload_fileobj.assert_not_called()
    downloader.tracker.increment.assert_any_call(Result.FAILED)
    # Lock in "at the source" — guard fires before any S3 lookup, even
    # when an existing object would otherwise be queried.
    downloader.s3_client.get_s3.assert_not_called()


# ---------------------------------------------------------------------------
# download_file_to_temp_path raises on empty body (HTTP 200 + 0 bytes)
# ---------------------------------------------------------------------------


def test_download_raises_on_zero_byte_response(downloader, tmp_path):
    # Simulates the OCLC ContentDM scenario that originally created the stubs:
    # HTTP 200 OK from the source, but the body delivers zero chunks (clean
    # close after headers, or empty content). The download must raise so
    # process_media skips the upload and the FAILED tracker fires.
    response = MagicMock()
    response.raise_for_status = MagicMock()  # no exception
    response.headers = {"content-length": "0"}
    response.iter_content.return_value = []  # no chunks delivered
    downloader.http_session.get.return_value = response

    local_file = tmp_path / "empty.jpg"
    import pytest

    with pytest.raises(RuntimeError, match="0 bytes"):
        downloader.download_file_to_temp_path(
            "http://example.com/x.jpg", str(local_file)
        )

    # The on-disk file should have been created (open(... "wb")) but stay empty
    assert local_file.exists()
    assert local_file.stat().st_size == 0


def test_download_succeeds_on_nonempty_response(downloader, tmp_path):
    # Sanity check: when chunks are delivered, no exception, file contains content.
    response = MagicMock()
    response.raise_for_status = MagicMock()
    response.headers = {"content-length": "12"}
    response.iter_content.return_value = [b"hello ", b"world!"]
    downloader.http_session.get.return_value = response

    local_file = tmp_path / "good.jpg"
    downloader.download_file_to_temp_path("http://example.com/x.jpg", str(local_file))

    assert local_file.read_bytes() == b"hello world!"


# ---------------------------------------------------------------------------
# Fix 2: HTTP session created once at init, reused per download
# ---------------------------------------------------------------------------


def test_http_session_created_once_at_init():
    from tools.downloader import Downloader

    web_mock = MagicMock()
    session = MagicMock()
    web_mock.get_http_session.return_value = session

    d = Downloader(
        provider="pa",
        tracker=MagicMock(),
        s3_client=MagicMock(),
        web=web_mock,
        local_fs=MagicMock(),
        iiif=MagicMock(),
    )

    web_mock.get_http_session.assert_called_once_with(provider="pa")
    assert d.http_session is session


def test_download_uses_stored_session(downloader):
    mock_response = MagicMock()
    mock_response.headers = {"content-length": "8"}
    mock_response.iter_content.return_value = [b"data" * 2]
    downloader.http_session.get.return_value = mock_response

    with patch("builtins.open", MagicMock()):
        downloader.download_file_to_temp_path("http://example.com/img.jpg", "/tmp/out")

    downloader.http_session.get.assert_called_once_with(
        "http://example.com/img.jpg", stream=True
    )


# ---------------------------------------------------------------------------
# Fix 3: IIIF manifest fetch skipped when file list is cached
# ---------------------------------------------------------------------------


def _staged_metadata(field: str, value) -> str:
    return json.dumps({"_staged_by_get_ids_es": True, field: value})


def test_process_item_uses_cached_file_list(downloader):
    cached_urls = ["http://example.com/page1.jpg", "http://example.com/page2.jpg"]

    downloader.s3_client.get_item_metadata.return_value = _staged_metadata(
        IIIF_MANIFEST_FIELD_NAME, "http://example.com/manifest.json"
    )
    downloader.s3_client.get_file_list.return_value = cached_urls

    downloader.process_item(
        overwrite=False,
        dry_run=True,
        verbose=False,
        partner="bpl",
        dpla_id="abcd1234",
        sleep_secs=0,
    )

    downloader.iiif.get_iiif_manifest.assert_not_called()


def test_process_item_fetches_manifest_when_cache_empty(downloader):
    manifest = {
        "@context": "http://iiif.io/api/presentation/3/context.json",
        "items": [],
    }
    downloader.s3_client.get_item_metadata.return_value = _staged_metadata(
        IIIF_MANIFEST_FIELD_NAME, "http://example.com/manifest.json"
    )
    downloader.s3_client.get_file_list.return_value = []
    downloader.iiif.get_iiif_manifest.return_value = manifest
    downloader.iiif.get_iiif_urls.return_value = []

    downloader.process_item(
        overwrite=False,
        dry_run=True,
        verbose=False,
        partner="bpl",
        dpla_id="abcd1234",
        sleep_secs=0,
    )

    downloader.iiif.get_iiif_manifest.assert_called_once_with(
        "http://example.com/manifest.json"
    )


def test_process_item_fetches_manifest_when_overwrite(downloader):
    cached_urls = ["http://example.com/page1.jpg"]
    manifest = {
        "@context": "http://iiif.io/api/presentation/3/context.json",
        "items": [],
    }
    downloader.s3_client.get_item_metadata.return_value = _staged_metadata(
        IIIF_MANIFEST_FIELD_NAME, "http://example.com/manifest.json"
    )
    downloader.s3_client.get_file_list.return_value = cached_urls
    downloader.iiif.get_iiif_manifest.return_value = manifest
    downloader.iiif.get_iiif_urls.return_value = cached_urls

    downloader.process_item(
        overwrite=True,
        dry_run=True,
        verbose=False,
        partner="bpl",
        dpla_id="abcd1234",
        sleep_secs=0,
    )

    downloader.iiif.get_iiif_manifest.assert_called_once()


def test_process_item_media_master_skips_manifest(downloader):
    downloader.s3_client.get_item_metadata.return_value = _staged_metadata(
        MEDIA_MASTER_FIELD_NAME, ["http://example.com/file.jpg"]
    )

    downloader.process_item(
        overwrite=False,
        dry_run=True,
        verbose=False,
        partner="texas",
        dpla_id="abcd1234",
        sleep_secs=0,
    )

    downloader.iiif.get_iiif_manifest.assert_not_called()


# ---------------------------------------------------------------------------
# Fix: _s3_key_age_days treats 0-byte stubs as absent so the downloader
# re-attempts them instead of leaving the stub forever (the persistent stub
# previously poisoned the uploader's gap-squashing page-label counter).
# ---------------------------------------------------------------------------


from datetime import datetime, timedelta, timezone  # noqa: E402


def _age_obj(content_length, days_old):
    obj = MagicMock()
    obj.content_length = content_length
    obj.last_modified = datetime.now(tz=timezone.utc) - timedelta(days=days_old)
    return obj


def test_s3_key_age_days_returns_none_for_zero_byte_stub(downloader):
    obj = _age_obj(content_length=0, days_old=10)
    downloader.s3_client.get_s3.return_value.Object.return_value = obj
    assert downloader._s3_key_age_days("any/path") is None


def test_s3_key_age_days_returns_age_for_real_file(downloader):
    obj = _age_obj(content_length=12345, days_old=3)
    downloader.s3_client.get_s3.return_value.Object.return_value = obj
    age = downloader._s3_key_age_days("any/path")
    assert age is not None
    assert 2.9 < age < 3.1


def test_s3_key_age_days_returns_none_for_missing(downloader):
    from botocore.exceptions import ClientError

    downloader.s3_client.get_s3.return_value.Object.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
    )
    assert downloader._s3_key_age_days("missing/path") is None


def test_s3_key_age_days_propagates_non_404_errors(downloader):
    from botocore.exceptions import ClientError

    downloader.s3_client.get_s3.return_value.Object.side_effect = ClientError(
        {"Error": {"Code": "InternalError", "Message": "boom"}}, "HeadObject"
    )
    with pytest.raises(ClientError):
        downloader._s3_key_age_days("any/path")


# ---------------------------------------------------------------------------
# process_media returns a per-ordinal status code so process_item can emit
# a per-item summary line. The existing global tracker counters are still
# incremented; the return value is purely so callers can group results.
#
# Status codes:
#   "SKIPPED"   — S3 already had a fresh-enough key (no network work)
#   "FETCHED"   — first-time fresh download to S3
#   "REFRESHED" — S3 key was stale; re-downloaded and overwritten
#   "FAILED"    — the ordinal raised at some point
# ---------------------------------------------------------------------------


def test_process_media_returns_skipped_when_s3_has_fresh_key(downloader):
    """If the S3 key exists and is younger than max_age_days, return
    'SKIPPED' immediately and don't touch the network."""
    with patch.object(downloader, "_s3_key_age_days", return_value=10.0):
        status = downloader.process_media(
            partner="bpl",
            dpla_id="abc",
            ordinal=1,
            media_url="http://example.com/x.jpg",
            overwrite=False,
            max_age_days=30,
            sleep_secs=0,
        )
    assert status == "SKIPPED"
    downloader.tracker.increment.assert_called_with(Result.SKIPPED)


def test_process_media_returns_fetched_on_first_time_download(downloader, tmp_path):
    """When the S3 key doesn't exist, process_media downloads + uploads
    and returns 'FETCHED'. The 'Fetched' log line carries bytes + seconds
    so operators can spot real network work in the log."""
    temp_file = MagicMock()
    temp_file.name = str(tmp_path / "fake.jpg")
    downloader.local_fs.get_temp_file.return_value = temp_file
    downloader.local_fs.get_content_type.return_value = "image/jpeg"
    downloader.local_fs.get_file_hash.return_value = "deadbeef"

    with (
        patch.object(downloader, "_s3_key_age_days", return_value=None),
        patch.object(downloader, "download_file_to_temp_path"),
        patch.object(downloader, "upload_file_to_s3", return_value=True),
        patch("os.stat") as mock_stat,
        patch("logging.info") as mock_log,
    ):
        mock_stat.return_value.st_size = 123456
        status = downloader.process_media(
            partner="bpl",
            dpla_id="abc",
            ordinal=2,
            media_url="http://example.com/y.jpg",
            overwrite=False,
            max_age_days=30,
            sleep_secs=0,
        )

    assert status == "FETCHED"
    # The "Fetched" line must appear with byte count + elapsed seconds —
    # this is the additive companion to the pre-check "Downloading" line
    # that makes the log honest about which considerations were real
    # downloads.
    fetched_log = next(
        (c for c in mock_log.call_args_list if "Fetched bpl abc 2" in str(c)),
        None,
    )
    assert fetched_log is not None, (
        f"Expected a 'Fetched bpl abc 2 ...' log line; got: {mock_log.call_args_list}"
    )
    assert "123,456 bytes" in str(fetched_log)


def test_process_media_returns_refreshed_when_s3_key_stale(downloader, tmp_path):
    """When the S3 key exists but is older than max_age_days,
    process_media re-downloads and returns 'REFRESHED' (distinct from
    'FETCHED' so the per-item summary can break refresh out)."""
    temp_file = MagicMock()
    temp_file.name = str(tmp_path / "fake.jpg")
    downloader.local_fs.get_temp_file.return_value = temp_file
    downloader.local_fs.get_content_type.return_value = "image/jpeg"
    downloader.local_fs.get_file_hash.return_value = "deadbeef"

    with (
        patch.object(downloader, "_s3_key_age_days", return_value=400.0),
        patch.object(downloader, "download_file_to_temp_path"),
        patch.object(downloader, "upload_file_to_s3", return_value=True),
        patch("os.stat") as mock_stat,
    ):
        mock_stat.return_value.st_size = 42
        status = downloader.process_media(
            partner="bpl",
            dpla_id="abc",
            ordinal=3,
            media_url="http://example.com/z.jpg",
            overwrite=False,
            max_age_days=30,
            sleep_secs=0,
        )

    assert status == "REFRESHED"


def test_process_media_returns_failed_on_exception(downloader, tmp_path):
    """When the download itself raises, the status is 'FAILED' (and the
    global tracker is bumped). Lets process_item count failed ordinals
    distinctly in the per-item summary."""
    temp_file = MagicMock()
    temp_file.name = str(tmp_path / "fake.jpg")
    downloader.local_fs.get_temp_file.return_value = temp_file

    with (
        patch.object(downloader, "_s3_key_age_days", return_value=None),
        patch.object(
            downloader,
            "download_file_to_temp_path",
            side_effect=RuntimeError("network blew up"),
        ),
    ):
        status = downloader.process_media(
            partner="bpl",
            dpla_id="abc",
            ordinal=4,
            media_url="http://example.com/x.jpg",
            overwrite=False,
            max_age_days=30,
            sleep_secs=0,
        )

    assert status == "FAILED"
    downloader.tracker.increment.assert_any_call(Result.FAILED)


def test_process_item_emits_per_item_summary_line(downloader, tmp_path):
    """The ordinal loop must end with one summary line tallying skip /
    fetch / refresh / fail counts. Grep-friendly companion to the
    per-ordinal 'Fetched' line — operators can grep
    ``"Item .*fetched=[1-9]"`` to find items that actually had work."""
    from ingest_wikimedia.dpla import MEDIA_MASTER_FIELD_NAME

    downloader.s3_client.get_item_metadata.return_value = json.dumps(
        {
            "_staged_by_get_ids_es": True,
            MEDIA_MASTER_FIELD_NAME: [
                "http://example.com/page1.jpg",
                "http://example.com/page2.jpg",
                "http://example.com/page3.jpg",
            ],
        }
    )

    # Make all three ordinals resolve to SKIPPED so the loop completes
    # quickly without needing a full download mock chain.
    with (
        patch.object(downloader, "process_media", return_value="SKIPPED"),
        patch("logging.info") as mock_log,
    ):
        downloader.process_item(
            overwrite=False,
            dry_run=False,
            verbose=False,
            partner="bpl",
            dpla_id="item-xyz",
            sleep_secs=0,
        )

    summary_log = next(
        (c for c in mock_log.call_args_list if "Item item-xyz: 3 ordinals" in str(c)),
        None,
    )
    assert summary_log is not None, (
        f"Expected an 'Item item-xyz: 3 ordinals ...' summary log line; "
        f"got: {[str(c) for c in mock_log.call_args_list]}"
    )
    assert "skipped=3" in str(summary_log)
    assert "fetched=0" in str(summary_log)
    assert "refreshed=0" in str(summary_log)
    assert "failed=0" in str(summary_log)


def test_process_item_summary_groups_mixed_statuses(downloader, tmp_path):
    """A mix of SKIPPED / FETCHED / REFRESHED / FAILED outcomes across
    the ordinals of one item must be tallied correctly in the summary."""
    from ingest_wikimedia.dpla import MEDIA_MASTER_FIELD_NAME

    downloader.s3_client.get_item_metadata.return_value = json.dumps(
        {
            "_staged_by_get_ids_es": True,
            MEDIA_MASTER_FIELD_NAME: ["url" + str(i) for i in range(6)],
        }
    )

    # Per-ordinal status cycle: 3 skipped, 2 fetched, 1 refreshed
    statuses = ["SKIPPED", "SKIPPED", "SKIPPED", "FETCHED", "FETCHED", "REFRESHED"]
    with (
        patch.object(downloader, "process_media", side_effect=statuses),
        patch("logging.info") as mock_log,
    ):
        downloader.process_item(
            overwrite=False,
            dry_run=False,
            verbose=False,
            partner="bpl",
            dpla_id="item-mixed",
            sleep_secs=0,
        )

    summary_log = next(
        (c for c in mock_log.call_args_list if "Item item-mixed" in str(c)),
        None,
    )
    assert summary_log is not None
    text = str(summary_log)
    assert "6 ordinals" in text
    assert "skipped=3" in text
    assert "fetched=2" in text
    assert "refreshed=1" in text
    assert "failed=0" in text


def test_process_item_does_not_emit_summary_in_dry_run(downloader, tmp_path):
    """The per-item summary is meaningless during a dry run (no
    process_media calls happen, so all counts would be zero). Suppress
    it so dry-run logs aren't padded with empty summary lines."""
    from ingest_wikimedia.dpla import MEDIA_MASTER_FIELD_NAME

    downloader.s3_client.get_item_metadata.return_value = json.dumps(
        {
            "_staged_by_get_ids_es": True,
            MEDIA_MASTER_FIELD_NAME: ["url1", "url2"],
        }
    )

    with patch("logging.info") as mock_log:
        downloader.process_item(
            overwrite=False,
            dry_run=True,
            verbose=False,
            partner="bpl",
            dpla_id="item-dry",
            sleep_secs=0,
        )

    summary_logs = [c for c in mock_log.call_args_list if "Item item-dry" in str(c)]
    assert not summary_logs, (
        f"Per-item summary should be suppressed in dry-run mode; got: {summary_logs}"
    )
