import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
import threading


__thread_local = threading.local()
__thread_local.http_session = None


def get_http_session() -> requests.Session:
    """
    Returns an initialized Requests session for the current thread.
    """
    if __thread_local.http_session is not None:
        return __thread_local.http_session
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        respect_retry_after_header=True,
        raise_on_status=True,
        raise_on_redirect=True,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    __thread_local.http_session = session
    return session


HTTP_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
            (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
}
