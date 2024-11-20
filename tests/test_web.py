from unittest.mock import patch, MagicMock
from ingest_wikimedia.web import get_http_session


@patch("ingest_wikimedia.web.requests.Session")
def test_get_http_session(mock_session):
    mock_sess = MagicMock()
    mock_session.return_value = mock_sess

    session = get_http_session()
    assert session == mock_sess
    mock_session.assert_called_once()
