"""
IIIF utilities
"""
__author__ = "DPLA"
__version__ = "0.0.1"
__license__ = "MIT"

import json
import requests

from wikimedia.utilities.exceptions import IIIFException

class IIIF:
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

        canvases = []
        images_urls = []

        iiif_manifest = self._get_iiif_manifest(iiif)
        # if 'sequences' in iiif_manifest and there is one sequence value
        if 'sequences' in iiif_manifest and len(iiif_manifest['sequences']) == 1:
            canvases = iiif_manifest['sequences'][0]['canvases'] if 'canvases' in iiif_manifest['sequences'][0] else []
        else:
            # More than one sequence, return empty list and log some kind of message
            raise IIIFException(f"Got more than one IIIF sequence. Unsure of meaning. {iiif}")

        for canvas in canvases:
            try:
                image_url = canvas['images'][0]['resource']['@id']
                # if missing file extension add it to URL to be requested
                image_url = image_url if '.' in image_url[image_url.rfind('/'):] else f"{image_url}.jpg"
                images_urls.append(image_url)
            except KeyError as keyerr:
                raise IIIFException(f"No `image` key for: {iiif}") from keyerr
        return images_urls

    def _get_iiif_manifest(self, url):
        """
        :return: JSON object
        """
        try:
            request = requests.get(url, timeout=30)
            if request.status_code not in [200, 301, 302]:
                raise Exception(f"Unable to request {url}: {request.status_code}")
            data = request.content
            return json.loads(data)
        except ConnectionError as connection_error:
            raise Exception(f"Unable to request {url}: {str(connection_error)}") from connection_error
        except json.decoder.JSONDecodeError as json_decode_error:
            raise Exception(f"Unable to decode JSON from {url}: {str(json_decode_error)}") from json_decode_error