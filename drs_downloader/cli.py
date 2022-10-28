from DRSClient import DRSClient
from pathlib import Path
from typing import List
import aiohttp
import async_downloader.download as async_downloader
import asyncio
import click
import json
import logging
import os.path
import pandas as pd
import requests
import time

from drs_downloader.async_downloader.download import DownloadURL

"""
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True
"""
endpoint = 'https://development.aced-idp.org'
drs_api = '/ga4gh/drs/v1/objects?page=10'


class Gen3DRSClient(DRSClient):
    """Handles Gen3 specific authentication using Fence"""

    # Initialize a DRS Client for the service at the specified url base
    # and with the REST resource to provide an access key
    def __init__(self, api_url_base,  access_token_resource_path, api_key_path,
                 access_id=None, debug=False):
        super().__init__(api_url_base, access_id, debug=debug)
        self.api_key = None
        self.access_token_resource_path = access_token_resource_path
        self.api_key_path = api_key_path
        # self.authorize()

    def authorize(self):
        full_key_path = os.path.expanduser(self.api_key_path)
        try:
            with open(full_key_path) as f:
                self.api_key = json.load(f)
            code = self.updateAccessToken()
            if code == 401:
                print('Invalid access token in {}'.format(full_key_path))
                self.api_key = None
            elif code != 200:
                print('Error {} getting Access token for {}'.format(
                    code, self.api_url_base))
                print('Using {}'.format(full_key_path))
                self.api_key = None

        except:
            self.api_key = None

    # Obtain an access_token using the provided Fence API key.
    # The client object will retain the access key for subsequent calls
    def updateAccessToken(self):
        headers = {'Content-Type': 'application/json'}
        api_url = '{0}{1}'.format(
            self.api_url_base, self.access_token_resource_path)
        response = requests.post(api_url, headers=headers, json=self.api_key)

        if response.status_code == 200:
            resp = json.loads(response.content.decode('utf-8'))
            self.access_token = resp['access_token']
            self.authorized = True
        else:
            self.has_auth = False
        return response.status_code

    def get_access_url(self, object_id, access_id=None):
        if not self.authorized:
            self.authorize()
        return DRSClient.get_access_url(self, object_id, access_id=access_id)


class bdcDRSClient(Gen3DRSClient):
    # Mostly done by the Gen3DRSClient, this just deals with url and end point specifics
    def __init__(self, api_key_path, access_id=None,  debug=False):
        super().__init__('https://development.aced-idp.org',
                         '/user/credentials/cdis/access_token', api_key_path, access_id, debug)


@click.group(no_args_is_help=True)
def cli():
    """Welcome to the drs_downloader."""
    print(cli.__doc__)
    return True


async def create_download_url(session, url: str) -> DownloadURL:
    """Create an instance of DownloadURL with a download URL, md5 hash, and object size."""

    md5, size = None
    with session.get(url) as resp:
        response = await resp.json()
        md5 = response['checksums'][0]['checksum']
        size = response['size']

    downloadUrl = DownloadURL(url, md5, size)
    return downloadUrl


async def get_md5s_and_sizes(uris_file: str):
    """"""

    with open(uris_file, 'r') as f:
        uris = f.read().splitlines()
        whats_this = map(lambda x: endpoint + drs_api + x.split(':')[-1], uris)
        urls = [list_obj for list_obj in whats_this]

    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            url = tasks.append(asyncio.ensure_future(
                create_download_url(session, url)))
        md5s_and_sizes = await asyncio.gather(*tasks)
        return urls, md5s_and_sizes


def download(credentials: str, uris_file: str, dest: str):
    """Download DRS objects from a list of DRS ids."""
    urls = []

    # ID Parsing, md5/size requests
    start_1 = time.time()
    drs_client = bdcDRSClient(credentials, 's3', debug=True)
    uris, md5sandSizes = asyncio.run(get_md5s_and_sizes(uris_file))
    end_1 = time.time()
    print('total time to do id parsing and md5 and size http requests', end_1-start_1)

    # URL signing and authentication
    start_2 = time.time()
    download_url = get_signed_url(drs_client, uris)
    end_2 = time.time()
    print('total time to do the URL signing and authentication', end_2-start_2)

    # DRS object downloading
    start_3 = time.time()
    for md5andSize in md5sandSizes:
        download_url_bundle = async_downloader.DownloadURL(
            download_url[md5sandSizes.index(md5andSize)], md5andSize[0], md5andSize[1])
        urls.append(download_url_bundle)
    async_downloader.download(urls, Path('DATA'))
    end_3 = time.time()

    print("total time to do the downloading and bundling of the information", end_3-start_3)


def get_signed_url(drs_client, drs_ids: List[str]) -> List[str]:
    """Return a list of signed download URLs from a list of DRS objects"""
    test_data = {
        'BioDataCatalyst': {
            'drs_client': drs_client,
            'drs_ids': drs_ids
        }
    }

    urls = []
    for (testname, test) in test_data.items():
        drsClient = test['drs_client']
        for drs_id in test['drs_ids']:
            url = drsClient.get_access_url(drs_id)
            # print(url)
            urls.append(url)

    return urls


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


# @cli.command()
# @click.option('--id', default=None, show_default=True, help='URI of the file of interest')
def info(id: str) -> dict:
    """Displays information of a given DRS object.
    Send request to endpoint with the DRS object URI provided by the user.
    """

    response = {}
    url = endpoint + drs_api + id
    try:
        response = _send_request(url)
    except Exception:
        print("response call to _send_request(url) has failed in info function")

    return response


# @cli.command()
def list():
    """Lists all DRS Objects at a given endpoint."""

    count = 0
    num = 0
    url = endpoint + drs_api + str(num)
    response = _send_request(url)
    drs_objects = response['drs_objects']
    ids = []
    while (drs_objects):
        for drs_object in drs_objects:
            count = count+1
            id = drs_object['id']
            # print(id)
            if (drs_object and drs_object['access_methods'] and drs_object['access_methods'][0]['access_id']):
                ids.append(id)

        num = num+1
        url = endpoint + drs_api + str(num)
        response = _send_request(url)
        drs_objects = response['drs_objects']

    # write URIS too why not
    # with open('uris.txt', 'w') as uris:
    #    for id in ids:
    #        uris.write(id + '\n')

    print(ids)


def extract_tsv_information(tsv_path: str):
    """Downloads DRS objects with ID's from a given TSV file."""
    urls = []
    df = pd.read_csv(tsv_path, sep='\t')
    for i in range(5):
        print(df['file_sha256'][i])
        print(df['file_size'][i])
        print(df['file_url'][i])
        download_url = async_downloader.DownloadURL(
            df['file_url'][i], df['file_sha256'][i], df['file_size'][i])
        urls.append(download_url)

    async_downloader.download(urls, Path('TSV_DATA'))


if __name__ == '__main__':
    # Extract_TSV_Information('Terra Data - 1M Neurons 2022-10-25 03.36.tsv')
    start = time.time()
    download('~/Desktop/credentials.json', 'uris.txt', '')
    end = time.time()
    print('Total Execution time', end-start)
