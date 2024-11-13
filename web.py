import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

__http_session: requests.Session | None = None


def get_http_session() -> requests.Session:
    global __http_session
    if __http_session is not None:
        return __http_session
    retry_strategy = Retry(
        connect=3,
        read=3,
        redirect=5,
        status=5,
        other=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        respect_retry_after_header=True,
        raise_on_status=True,
        raise_on_redirect=True,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    __http_session = requests.Session()
    __http_session.mount("https://", adapter)
    __http_session.mount("http://", adapter)
    return __http_session


HTTP_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
            (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
}
