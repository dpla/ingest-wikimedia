import functools

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry


RETRY_COUNT = 3
RETRY_BACKOUT_FACTOR = 1
DEFAULT_CONN_TIMEOUT = 45


def get_http_session() -> requests.Session:
    """
    Returns an initialized Requests session.
    """
    retry_strategy = Retry(
        total=RETRY_COUNT,
        backoff_factor=RETRY_BACKOUT_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        respect_retry_after_header=True,
        raise_on_status=True,
        raise_on_redirect=True,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    # To have a default session-level connection init timeout,
    # you have to result to this:
    session.get = functools.partial(session.get, timeout=DEFAULT_CONN_TIMEOUT)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


HTTP_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) "
    "Gecko/20100101 Firefox/135.0"
}
