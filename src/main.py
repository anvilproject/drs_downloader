
from download import DownloadURL
from Gen3DRSClient import Gen3DRSClient
from pathlib import Path
from typing import List
import csv
import json
import requests

endpoint = 'https://development.aced-idp.org/ga4gh/drs/v1/objects'


def download(tsv_file: str, dest: str):
    """Download DRS objects from a list of DRS ids."""

    urls = []

    # Read in DRS URI's from TSV file. Check for hash and sizes as well.

    # URL signing and authentication
    download_url = get_signed_url(drs_client, uris)

    # DRS object downloading
    for md5andSize in md5sandSizes:
        download_url_bundle = async_downloader.DownloadURL(
            download_url[md5sandSizes.index(md5andSize)], md5andSize[0], md5andSize[1])
        urls.append(download_url_bundle)
    async_downloader.download(urls, Path('DATA'))


def get_signed_url(drs_uri: str) -> List[str]:
    """Return a signed download URL for a given DRS object"""
    id = drs_uri.split(':')[-1]
    response = _send_request(endpoint + id) 

    url = response['access_methods'][0]['access_url']
    return url

def _create_download_url(url: str) -> DownloadURL:
    """Create an instance of DownloadURL with a download URL, md5 hash, and object size."""

    response = _send_request(url)
    md5 = response['checksums'][0]['checksum']
    size = response['size']

    downloadUrl = DownloadURL(url, md5, size)
    return downloadUrl


def _send_request(url: str) -> dict:
    """Sends a GET request to a given URL. Returns the response in JSON format."""
    json_resp = {}
    try:
        response = requests.get(url)
        response.raise_for_status()
        resp = response.content.decode('utf-8')
        json_resp = json.loads(resp)

    except Exception:
        print("exception has occurred in _send_request")

    return json_resp


def _extract_tsv_information(tsv_file: str) -> List[DownloadURL]:
    """Downloads DRS objects with ID's from a given TSV file."""
    
    urls = []
    
    with open(tsv_file) as tsv:
        # Use DictReader here to skip header row
        data = csv.DictReader(tsv, delimiter='\t')
        for row in data:
            md5 = row['file_sha256']
            size = row['file_size']
            drs_uri = row['file_drs_uri']

if __name__ == '__main__':
    _extract_tsv_information('tests/terra-data.tsv')
