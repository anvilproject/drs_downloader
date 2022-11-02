import json
from typing import List
from drs_downloader.download import download, DownloadURL
from pathlib import Path
import pandas as pd
import aiohttp
import asyncio
import subprocess
import sys


def _extract_tsv_info(tsv_path: str, drs_header: str) -> List[str]:
    """Extract the DRS URI's from the provided TSV file.

    Args:
        tsv_path (str): The input file containing a list of DRS URI's.
        drs_header (str): Column header for the DRS URI's.

    Returns:
        List[str]: The URI's corresponding to the DRS objects.
    """
    uris = []

    df = pd.read_csv(tsv_path, sep='\t')
    if (drs_header in df.columns.values.tolist()):
        for i in range(df[drs_header].count()):
            uris.append(df['pfb:ga4gh_drs_uri'][i])
    else:
        raise KeyError(f"Header '{drs_header}' not found in {tsv_path}")

    return uris


def _get_auth_token() -> str:
    """Get Google Cloud authentication token.
    User must run 'gcloud auth login' from the shell before starting this script.

    Returns:
        str: auth token
    """
    token_command = "gcloud auth print-access-token"
    cmd = token_command.split(' ')
    token = subprocess.check_output(cmd).decode("ascii")[0:-1]
    return token


async def _get_download_urls(uris: List[str]) -> List[DownloadURL]:
    """Create DownloadURL instances containing a signed URL, an md5 hash, and file size.

    Args:
        uris (List[str]): A list of DRS URI's

    Returns:
        List[DownloadURL]: The DownloadURL instances ready for the async downloader
    """
    url_endpoint = "https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v3"
    token = _get_auth_token()
    header = {
        'authorization': 'Bearer ' + token,
        'content-type': 'application/json'
    }
    async with aiohttp.ClientSession(headers=header) as session:
        download_urls = await asyncio.gather(*[_get_more(session, uri, url_endpoint) for uri in uris])
        return download_urls


async def _get_more(session: aiohttp.ClientSession, uri: str, endpoint: str) -> DownloadURL:
    """Sends a POST request for the signed URL, hash, and file size of a given DRS object.

    Args:
        session (aiohttp.ClientSession): session with the DRS server
        uri (str): DRS URI
        endpoint (str): URL of the DRS compatible server

    Raises:
        Exception: The request was rejected by the server

    Returns:
        DownloadURL: The downloadable bundle ready for async download
    """
    data = {
        "url": uri,
        "fields": ["fileName", "size", "hashes", "accessUrl"]
    }

    async with session.post(url=endpoint, data=json.dumps(data)) as response:
        resp = await response.json(content_type=None)

        if (resp['accessUrl'] is None):
            raise Exception("A valid URL was not returned from the server.")

        url = resp['accessUrl']['url']
        md5 = resp['hashes']['md5']
        size = resp['size']
        download_url = DownloadURL(url, md5, size)

        return download_url


def download_files(tsv_file: str, drs_header: str, dest: str):
    """Download DRS objects from a list of DRS ids.

    Args:
        tsv_file (str): Path to the input TSV file.
        drs_header (str): Column header for the DRS URI's.
        dest (str): Directory to download the DRS objects to.
    """

    # Read in DRS URI's from TSV file. Check for hash and sizes as well.
    dest_dir = Path(dest)
    if (dest_dir.is_dir() is False):
        dest_dir.mkdir(parents=True)

    uris = _extract_tsv_info(tsv_file, drs_header)
    download_urls = asyncio.run(_get_download_urls(uris))
    download(download_urls, dest_dir)


def main():
    print('Welcome to the DRS Downloader!\n')

    tsv_file = ''
    dest = ''

    if len(sys.argv) > 1:
        tsv_file = sys.argv[1]
        drs_header = sys.argv[2]
        dest = sys.argv[3]

    else:
        tsv_file = input('Path of TSV file (ex. terra-data.tsv): ')
        drs_header = input('DRS Header (ex. pfb:ga4gh_drs_uri): ')
        dest = input('Path to download to (ex. /home/DATA): ')

    print(f'Beginning download to {dest}')
    download_files(tsv_file, drs_header, dest)
    print('Downloading complete!')


if __name__ == '__main__':
    main()
