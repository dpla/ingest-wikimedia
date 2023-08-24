"""
IIIF utilities
"""
import json

import requests
import validators
from utilities.exceptions import IIIFException


class IIIF:
    HEADERS = {'User-Agent':
               'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36' }

    """
    """
    def __init__(self):
        pass

    # IIIF Manifest functions
    def iiif_v2_urls(self, iiif):
        """
        Extracts image URLs from IIIF manfiest and returns them as a list
        # TODO
        """

    def iiif__v3_urls(self, iiif):
        """
        Needs to be implemented for Georgia uploads to Wikimedia Commons
        To be done by October 2023
        # TODO
        """

    def get_iiif_urls(self, iiif):
        """
        Extracts image URLs from IIIF manfiest and returns them as a list
        Currently only supports IIIF v2

        :param iiif: IIIF manifest URL
        :return: List of image URLs
        """

        manifest = self._get_iiif_manifest(iiif)

        urls = []
        sequences = manifest.get('sequences', [])
        sequence = sequences[0:1] if len(sequences) == 1 else None
        canvases = sequence[0].get('canvases', []) if sequence else  []
        for canvase in canvases:
            for image in canvase.get('images', []):
                url = image.get('resource', {}).get('@id', None)
                if url:
                    urls.append(url)
        return urls

    def _get_iiif_manifest(self, url):
        """
        :return: JSON object
        """
        if not validators.url(url):
            raise IIIFException(f"Invalid url {url}")
        try:
            request = requests.get(url, timeout=30, headers=self.HEADERS)
            if request.status_code not in [200, 301, 302]:
                raise IIIFException(f"Unable to request: {url} - Status code {request.status_code}")
            data = request.content
            return json.loads(data)
        except json.decoder.JSONDecodeError as json_decode_error:
            raise IIIFException(f"Unable to decode JSON: {url} -- {str(json_decode_error)}") from json_decode_error
        except requests.exceptions.RequestException as re:
            raise IIIFException(f"Unable to request: {url} -- {str(re)}") from re
