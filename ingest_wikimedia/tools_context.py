import tomllib


from ingest_wikimedia.banlist import Banlist
from ingest_wikimedia.iiif import IIIF
from ingest_wikimedia.localfs import LocalFS
from ingest_wikimedia.dpla import DPLA
from ingest_wikimedia.s3 import S3Client
from ingest_wikimedia.tracker import Tracker
from ingest_wikimedia.web import Web


class ToolsContext:
    """
    A class to manage the context of the tools environment.
    Mainly so we don't need repetitive code to initialize
    everything in the tools.
    """

    def __init__(
        self,
        tracker: Tracker,
        s3_client: S3Client,
        web: Web,
        local_fs: LocalFS,
        dpla: DPLA,
        iiif: IIIF,
    ) -> None:
        self._tracker = tracker
        self._s3_client = s3_client
        self._web = web
        self._local_fs = local_fs
        self._iiif = iiif
        self._dpla = dpla

    def get_tracker(self) -> Tracker:
        return self._tracker

    def get_s3_client(self) -> S3Client:
        return self._s3_client

    def get_web(self) -> Web:
        return self._web

    def get_local_fs(self) -> LocalFS:
        return self._local_fs

    def get_dpla(self) -> DPLA:
        return self._dpla

    def get_iiif(self) -> IIIF:
        return self._iiif

    @staticmethod
    def init(provider) -> "ToolsContext":
        with open("config.toml", "rb") as f:
            config = tomllib.load(f)

        tracker = Tracker()
        s3_client = S3Client()
        web = Web(config[provider].secret)
        http_session = web.get_http_session(provider)
        local_fs = LocalFS()
        banlist = Banlist()
        iiif = IIIF(tracker, http_session)
        dpla = DPLA(config["api_key"], tracker, http_session, s3_client, banlist, iiif)

        return ToolsContext(
            tracker=tracker,
            s3_client=s3_client,
            web=web,
            local_fs=local_fs,
            iiif=iiif,
            dpla=dpla,
        )
