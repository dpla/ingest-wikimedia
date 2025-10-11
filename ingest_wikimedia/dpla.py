import json
import logging
import sys
from urllib import parse
from typing import TypeVar, Callable

from requests import Session

from .banlist import Banlist
from .common import null_safe, get_str, get_list, get_dict
from .iiif import IIIF
from .s3 import S3Client
from .tracker import Tracker, Result


T = TypeVar("T")


class DPLA:
    def __init__(
        self,
        api_key: str,
        tracker: Tracker,
        http_session: Session,
        s3_client: S3Client,
        banlist: Banlist,
        iiif: IIIF,
    ) -> None:
        self.api_key = api_key
        self.tracker = tracker
        self.http_session = http_session
        self.s3_client = s3_client
        self.banlist = banlist
        self.iiif = iiif

    @staticmethod
    def check_partner(partner: str) -> None:
        """
        Blows up if we're working on a partner we shouldn't.
        """
        if partner not in DPLA_PARTNERS.keys():
            raise ValueError(f"Unrecognized partner: {partner}")

    def get_nara_ids(self):
        def build_collections_params(http_session: Session, api_key: str) -> list[str]:
            _request_url = (
                "https://api.dp.la/v2/items"
                "?provider.name=%22National%20Archives%20and%20Records%20Administration%22"
                f"&api_key={api_key}"
                "&page_size=0"
                "&facet_size=50000"
                '&sourceResource.collection.title=NOT%20"Records%20of*"%20NOT%20"Naval%20Records%20Collection%20of%20the%20Office%20of%20Naval%20Records%20and%20Library"%20NOT%20"War%20Department%20Collection%20of%20Confederate%20Records"'
                "&facets=sourceResource.collection.title"
            )
            collection_facet_response = http_session.get(_request_url).json()

            return [
                "exact_field_match=true&sourceResource.collection.title="
                + parse.quote('"' + collection["term"] + '"', safe="")
                for collection in collection_facet_response["facets"][
                    "sourceResource.collection.title"
                ]["terms"]
                if (collection["count"] < 50000)
                and ("Personnel" not in collection["term"])
                and ("Military Files" not in collection["term"])
                and ("Correspondence Files" not in collection["term"])
                and (
                    "War Department Collection of Revolutionary War Records"
                    not in collection["term"]
                )
            ]

        def build_languages_params(http_session: Session, api_key: str) -> list[str]:
            _request_url = (
                "https://api.dp.la/v2/items"
                "?provider.name=%22National%20Archives%20and%20Records%20Administration%22"
                f"&api_key={api_key}"
                "&page_size=0"
                "&facets=sourceResource.language.name"
                "&facet_size=50000"
            )

            lang_facet_response = http_session.get(_request_url).json()

            lang_values = [
                '"' + lang["term"] + '"'
                for lang in lang_facet_response["facets"][
                    "sourceResource.language.name"
                ]["terms"]
                if lang["term"] != "English"
            ]

            return [
                "sourceResource.language.name=" + "+OR+".join(lang_values[i : i + 10])
                for i in range(0, len(lang_values), 10)
            ]

        def build_formats_params(http_session: Session, api_key: str) -> list[str]:
            _request_url = (
                "https://api.dp.la/v2/items"
                "?provider.name=%22National%20Archives%20and%20Records%20Administration%22"
                f"&api_key={api_key}"
                "&page_size=0"
                "&facets=sourceResource.format"
                "&facet_size=50000"
            )
            format_facet_response = http_session.get(_request_url).json()

            format_values = [
                '"' + facet["term"] + '"'
                for facet in format_facet_response["facets"]["sourceResource.format"][
                    "terms"
                ]
                if facet["count"] < 12000
            ]

            return [
                "sourceResource.format=" + "+OR+".join(format_values[i : i + 6])
                for i in range(0, len(format_values), 6)
            ]

        queries = []
        queries.extend(build_languages_params(self.http_session, self.api_key))
        queries.extend(build_formats_params(self.http_session, self.api_key))
        queries.extend(build_collections_params(self.http_session, self.api_key))

        for query in queries:
            has_results = True
            page = 0
            base_request_url = (
                "https://api.dp.la/v2/items"
                "?provider.name=%22National%20Archives%20and%20Records%20Administration%22"
                "&page_size=5000"
                f"&api_key={self.api_key}"
                "&rightsCategory=%22Unlimited+Re-Use%22"
                "&fields=id&" + query
            )
            while has_results:
                page += 1
                request_url = base_request_url + "&page=" + str(page)
                try:
                    res = self.http_session.get(request_url).json()

                    for item in res["docs"]:
                        yield item["id"]

                    if res["count"] <= (res["limit"] + res["start"]):
                        has_results = False
                except Exception as e:
                    raise RuntimeError(
                        "Error in request: "
                        + request_url.replace(self.api_key, "[REDACTED]")
                    ) from e

    def get_ids(self, partner: str, add_query: str | None, no_shard: bool):
        partner_full = DPLA_PARTNERS[partner]
        partner_string = partner_full.replace(" ", "+")

        api_query_base = (
            f"https://api.dp.la/v2/items?api_key={self.api_key}"
            f"&provider.name={partner_string}"
            "&rightsCategory=Unlimited+Re-Use"
            "&fields=id"
            "&page_size=500"
        )

        if add_query:
            api_query_base += "&" + add_query

        def run_query(query_url: str):
            page = 0
            while True:
                page += 1
                page_url = query_url + "&page=" + str(page)
                print(page, file=sys.stderr)
                response = self.http_session.get(page_url)
                response.raise_for_status()
                data = response.json()
                if not data.get("docs", None):
                    break
                for doc in data.get("docs"):
                    dpla_id = doc.get("id")
                    print(dpla_id)

        if not no_shard:
            for shard in [hex(i)[2:].zfill(2) for i in range(256)]:
                run_query(f"{api_query_base}&id={shard}*")
        else:
            run_query(api_query_base)

    def get_item_metadata(self, dpla_id: str) -> dict:
        """
        Retrieves a DPLA MAP record from the DPLA API for an item.
        """
        url = DPLA_API_URL_BASE + dpla_id
        headers = {AUTHORIZATION_HEADER: self.api_key}
        response = self.http_session.get(url, headers=headers)
        response.raise_for_status()
        response_json = response.json()
        docs = get_list(response_json, DPLA_API_DOCS)
        return docs[0] if docs else {}

    @staticmethod
    def check_record_partner(partner: str, item_metadata: dict) -> bool:
        partner_long_name = DPLA_PARTNERS.get(partner, "")
        record_partner_long_name = get_str(
            get_dict(item_metadata, PROVIDER_FIELD_NAME), EDM_AGENT_NAME
        )
        return partner_long_name == record_partner_long_name

    def is_wiki_eligible(
        self,
        dpla_id: str,
        item_metadata: dict,
        provider: dict,
        data_provider: dict,
    ) -> bool:
        """
        Enforces a number of criteria for ensuring this is an item we should upload.
        """

        def value_ok(value: T, test: Callable[[T], bool], error_msg: str):
            result = test(value)
            if not result:
                logging.info(error_msg)
            return result

        def non_empty_str(x: str) -> bool:
            return x != ""

        def not_false(x: bool) -> bool:
            return bool(x)

        def rights_category_check(x: str) -> bool:
            return x == UNLIMITED_RE_USE

        provider_wikidata_id_ok = value_ok(
            value=get_str(provider, WIKIDATA_FIELD_NAME),
            test=non_empty_str,
            error_msg="Missing wikidata id for provider.",
        )

        data_provider_wikidata_id_ok = value_ok(
            value=get_str(data_provider, WIKIDATA_FIELD_NAME),
            test=non_empty_str,
            error_msg="Missing wikidata id for dataProvider.",
        )

        wikidata_ids_ok = data_provider_wikidata_id_ok and provider_wikidata_id_ok

        provider_ok = value_ok(
            value=(
                null_safe(provider, UPLOAD_FIELD_NAME, False)
                or null_safe(data_provider, UPLOAD_FIELD_NAME, False)
            ),
            test=not_false,
            error_msg="Bad provider.",
        )

        rights_category_ok = value_ok(
            value=get_str(item_metadata, RIGHTS_CATEGORY_FIELD_NAME),
            test=rights_category_check,
            error_msg="Bad rights category.",
        )

        is_shown_at = get_str(item_metadata, EDM_IS_SHOWN_AT)
        media_master = len(get_list(item_metadata, MEDIA_MASTER_FIELD_NAME)) > 0
        iiif_manifest = get_str(item_metadata, IIIF_MANIFEST_FIELD_NAME)

        if not iiif_manifest and not media_master:
            iiif_url = self.iiif.contentdm_iiif_url(is_shown_at)
            if iiif_url is not None:
                response = self.http_session.head(iiif_url, allow_redirects=True)
                if response.status_code < 400:
                    item_metadata[IIIF_MANIFEST_FIELD_NAME] = iiif_url
                    iiif_manifest = iiif_url

        asset_ok = value_ok(
            value=(media_master or bool(str(iiif_manifest))),
            test=not_false,
            error_msg="Bad asset.",
        )

        dpla_id_ok = value_ok(
            value=not self.banlist.is_banned(dpla_id),
            test=not_false,
            error_msg="DPLA ID in banlist.",
        )

        return (
            rights_category_ok
            and asset_ok
            and provider_ok
            and dpla_id_ok
            and wikidata_ids_ok
        )

    @staticmethod
    def get_provider_and_data_provider(
        item_metadata: dict, providers_json: dict
    ) -> tuple[dict, dict]:
        """
        Loads metadata about the provider and data provider from the providers json file.
        """
        provider_name = get_str(
            get_dict(item_metadata, PROVIDER_FIELD_NAME), EDM_AGENT_NAME
        )
        data_provider_name = get_str(
            get_dict(item_metadata, DATA_PROVIDER_FIELD_NAME), EDM_AGENT_NAME
        )
        provider = get_dict(providers_json, provider_name)
        data_provider = get_dict(
            get_dict(provider, INSTITUTIONS_FIELD_NAME), data_provider_name
        )
        return provider, data_provider

    def get_providers_data(self) -> dict:
        """Loads the institutions file from ingestion3 in GitHub."""
        return self.http_session.get(INSTITUTIONS_URL).json()

    def extract_urls(
        self,
        partner,
        dpla_id,
        item_metadata: dict,
    ) -> list[str]:
        """
        Tries to find some way to get a list of file urls out of the item. Writes the IIIF
        manifest to S3 if there is one.
        """
        if MEDIA_MASTER_FIELD_NAME in item_metadata:
            return get_list(item_metadata, MEDIA_MASTER_FIELD_NAME)

        elif IIIF_MANIFEST_FIELD_NAME in item_metadata:
            manifest_url = get_str(item_metadata, IIIF_MANIFEST_FIELD_NAME)
            manifest = self.iiif.get_iiif_manifest(manifest_url)
            if manifest:
                self.s3_client.write_iiif_manifest(
                    partner, dpla_id, json.dumps(manifest)
                )
                return self.iiif.get_iiif_urls(manifest)
            else:
                self.tracker.increment(Result.BAD_IIIF_MANIFEST)
                raise ValueError(f"Bad IIIF manifest at {manifest_url}")

        else:
            self.tracker.increment(Result.NO_MEDIA)
            raise NotImplementedError(
                f"No {MEDIA_MASTER_FIELD_NAME} or {IIIF_MANIFEST_FIELD_NAME}"
            )

    @staticmethod
    def provider_str(provider: dict) -> str:
        """
        Creates a human-readable string out of the provider record.
        """
        return (
            f"Provider: {provider.get(WIKIDATA_FIELD_NAME, '')}, "
            f"{provider.get(UPLOAD_FIELD_NAME, '')}"
        )


DPLA_API_URL_BASE = "https://api.dp.la/v2/items/"
DPLA_API_DOCS = "docs"
INSTITUTIONS_URL = (
    "https://raw.githubusercontent.com/dpla/ingestion3"
    "/refs/heads/main/src/main/resources/wiki/institutions_v2.json"
)
UPLOAD_FIELD_NAME = "upload"
INSTITUTIONS_FIELD_NAME = "institutions"
SOURCE_RESOURCE_FIELD_NAME = "sourceResource"
MEDIA_MASTER_FIELD_NAME = "mediaMaster"
IIIF_MANIFEST_FIELD_NAME = "iiifManifest"
PROVIDER_FIELD_NAME = "provider"
DATA_PROVIDER_FIELD_NAME = "dataProvider"
EXACT_MATCH_FIELD_NAME = "exactMatch"
EDM_AGENT_NAME = "name"
EDM_IS_SHOWN_AT = "isShownAt"
RIGHTS_CATEGORY_FIELD_NAME = "rightsCategory"
EDM_RIGHTS_FIELD_NAME = "rights"
EDM_TIMESPAN_DISPLAY_DATE = "displayDate"
UNLIMITED_RE_USE = "Unlimited Re-Use"
DC_CREATOR_FIELD_NAME = "creator"
DC_DATE_FIELD_NAME = "date"
DC_DESCRIPTION_FIELD_NAME = "description"
DC_TITLE_FIELD_NAME = "title"
DC_IDENTIFIER_FIELD_NAME = "identifier"
WIKIDATA_FIELD_NAME = "Wikidata"
AUTHORIZATION_HEADER = "Authorization"

DPLA_PARTNERS = {
    "bpl": "Digital Commonwealth",
    "georgia": "Digital Library of Georgia",
    # hold off on illinois "il": "Illinois Digital Heritage Hub",
    "indiana": "Indiana Memory",
    "nara": "National Archives and Records Administration",
    "northwest-heritage": "Northwest Digital Heritage",
    "ohio": "Ohio Digital Network",
    "p2p": "Plains to Peaks Collective",
    "pa": "PA Digital",
    "texas": "The Portal to Texas History",
    "minnesota": "Minnesota Digital Library",
    "mwdl": "Mountain West Digital Library",
    "heartland": "Heartland Hub",
}
