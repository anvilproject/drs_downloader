from download import DownloadURL, download
from Gen3DRSClient import bdcDRSClient
from pathlib import Path
from typing import List
import csv
import json
import requests

_endpoint = 'https://development.aced-idp.org/ga4gh/drs/v1/objects/'


def download_drs(tsv_file: str, dest: str):
    """Download DRS objects from a list of DRS ids.

    Args:
        tsv_file (str): Path to the input TSV file.
        dest (str): Directory to download the DRS objects to.
    """

    # Read in DRS URI's from TSV file. Check for hash and sizes as well.
    uris = _extract_tsv_info(tsv_file)

    # URL signing and authentication
    downloadUrls = []
    for uri in uris:
        downloadUrl = _create_download_url(uri)
        downloadUrls.append(downloadUrl)

    # DRS object downloading
    dest = Path(dest)
    if (dest.is_dir() is False):
        dest.mkdir(parents=True)
    download(downloadUrls, dest)


def _get_signed_url(url: str) -> str:
    """Return a signed URL used to download a DRS object.

    Args:
        url (str): The base URL for a given DRS object.

    Returns:
        str: The signed URL for the DRS object.
    """
    client = bdcDRSClient('Secrets/credentials.json')
    signedUrl = client.get_access_url(url, 's3')

    return signedUrl


def _create_download_url(uri: str) -> DownloadURL:
    """Return a signed download URL for a given DRS object.

    Args:
        uri (str): The DRS ID to download.

    Returns:
        DownloadURL: The DownloadURL instance for the given DRS object.
    """

    id = uri.split(':')[-1]
    object_url = _endpoint + id
    response = send_request(object_url)

    response = send_request(object_url)
    md5 = response['checksums'][0]['checksum']
    size = response['size']

    signedUrl = _get_signed_url(object_url)

    # Create an instance of DownloadURL with a download URL, md5 hash, and
    # object size.
    downloadUrl = DownloadURL(signedUrl, md5, size)
    return downloadUrl


def send_request(url: str) -> dict:
    """Send a GET request to a given URL.

    Args:
        url (str): The URL to query.

    Returns:
        dict: The response in JSON format.
    """
    json_resp = {}
    try:
        response = requests.get(url)
        response.raise_for_status()
        resp = response.content.decode('utf-8')
        json_resp = json.loads(resp)

    except Exception:
        print("exception has occurred in _send_request")

    return json_resp


def _extract_tsv_info(tsv_file: str) -> List[str]:
    """Extract the DRS URI's from the provided TSV file.

    Args:
        tsv_file (str): The input file with a 'file_drs_uri' header.

    Returns:
        List[str]: The URI's corresponding to the DRS objects.
    """

    uris = []

    with open(tsv_file) as tsv:
        # Use DictReader here to skip header row
        data = csv.DictReader(tsv, delimiter='\t')
        for row in data:
            uri = row['file_drs_uri']
            uris.append(uri)

    return uris


if __name__ == '__main__':
    download_drs('tests/gen3-data.tsv', "/tmp/DATA")
