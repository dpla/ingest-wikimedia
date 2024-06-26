"""
IIIF utilities
"""

import json

import requests
import validators
from wikimedia.utilities.exceptions import IIIFException


class IIIF:
    """
    IIIF utilities"""

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
                (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
    }

    def __init__(self):
        pass

    # IIIF Manifest functions
    def iiif_v2_urls(self, iiif):
        """
        Extracts image URLs from IIIF manfiest and returns them as a list
        """
        urls = []
        sequences = iiif.get("sequences", [])
        sequence = sequences[0:1] if len(sequences) == 1 else None
        canvases = sequence[0].get("canvases", []) if sequence else []

        for canvase in canvases:
            for image in canvase.get("images", []):
                url = image.get("resource", {}).get("@id", None)
                if url:
                    urls.append(url)
        return urls

    def iiif__v3_urls(self, iiif):
        """ """
        # items[0] \ items[x] \ items[0] \ body \ id
        resolution = "/full/full/0/default.jpg"
        urls = []
        for item in iiif.get("items", []):
            try:
                url = item["items"][0]["items"][0].get("body", {}).get("id", None)
                # This is a hack to get around that v3 presumes the user supplies the
                # resolution in the URL
                if url:
                    # This condition may not be necessary but I'm leaving it in for now
                    if url.endswith("default.jpg"):
                        urls.append(url)
                    else:
                        urls.append(f"{url}{resolution}")
            except (IndexError, TypeError, KeyError):
                return []
        return urls

    def get_iiif_urls(self, iiif):
        """
        Extracts image URLs from IIIF manfiest and returns them as a list
        Currently only supports IIIF v2

        :param iiif: IIIF manifest URL
        :return: List of image URLs
        """

        manifest = self._get_iiif_manifest(iiif)

        # v2 or v3?
        if (
            manifest.get("@context", None)
            == "http://iiif.io/api/presentation/3/context.json"
        ):
            return self.iiif__v3_urls(manifest)
        elif (
            manifest.get("@context", None)
            == "http://iiif.io/api/presentation/2/context.json"
        ):
            return self.iiif_v2_urls(manifest)
        else:
            raise IIIFException("Unknown IIIF version")

    def _get_iiif_manifest(self, url):
        """
        :return: JSON object
        """
        if not validators.url(url):
            raise IIIFException(f"Invalid url {url}")
        try:
            request = requests.get(
                url, timeout=30, allow_redirects=True, headers=self.HEADERS
            )
            if request.status_code not in [200, 301, 302]:
                raise IIIFException(f"Invalid response code: {request.status_code}")
            data = request.content
            return json.loads(data)
        except json.decoder.JSONDecodeError as jdex:
            raise IIIFException(f"Unable to decode JSON: {url} - {str(jdex)}") from jdex
        except requests.exceptions.RequestException as re:
            raise IIIFException(f"{str(re)}") from re
        except Exception as ex:
            raise Exception(f"Unknown error: {str(ex)}") from ex
