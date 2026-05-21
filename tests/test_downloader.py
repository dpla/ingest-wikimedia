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
