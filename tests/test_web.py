import functools
from unittest.mock import patch, MagicMock
from ingest_wikimedia.web import Web


@patch("ingest_wikimedia.web.requests.Session")
def test_get_http_session(mock_session):
    mock_sess = MagicMock()
    mock_session.return_value = mock_sess
    web = Web({"provider": "secret"})
    session = web.get_http_session("provider")
    assert session == mock_sess
    mock_session.assert_called_once()
    patch.stopall()


def test_exercise_monkey_patched_session():
    web = Web({"provider": "secret"})
    session = web.get_http_session("provider")
    assert isinstance(session.get, functools.partial)
    response = session.get("https://example.com")
    assert response.status_code == 200


def test_headers():
    web = Web({"provider": "secret"})
    session = web.get_http_session("provider")
    headers = session.headers

    assert "User-Agent" in headers
    assert str(headers["User-Agent"]).startswith("Mozilla")  # NOQA
    assert "X-DPLA-Bot-Authorization" in headers
    assert headers["X-DPLA-Bot-Authorization"] == "Basic secret"
    assert "X-DPLA-Bot-ID" in headers
    assert headers["X-DPLA-Bot-ID"] == "wikimedia-ingest"
