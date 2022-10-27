from DRSClient import DRSClient
from pathlib import Path
from re import I
import async_downloader.download as async_downloader
import click
import json
import os.path
import requests
from typing import List


endpoint = 'https://development.aced-idp.org'
drs_api = '/ga4gh/drs/v1/objects/'


class Gen3DRSClient(DRSClient):
    '''Handles Gen3 specific authentication using Fence'''

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


class anvilDRSClient(Gen3DRSClient):

    def __init__(self, api_key_path, userProject=None, access_id=None,  debug=False):
        self.userProject = userProject
        super().__init__('https://gen3.theanvil.io',
                         '/user/credentials/api/access_token', api_key_path, access_id, debug)

    # Get a URL for fetching bytes.
    # Anvil GCP resources requires you to provide the userAccount to which charges will be accrued
    # That user account must grant serviceusage.services.use access to your anvil service account
    # e.g. to user-123@anvilprod.iam.gserviceaccount.com
    def get_access_url(self, object_id, access_id=None):
        result = super().get_access_url(object_id, access_id)

        if result != None:
            if self.userProject == None:
                return result
            else:
                return '{}&userProject={}'.format(result, self.userProject)
        else:
            return None


@click.group(no_args_is_help=True)
def cli():
    """Welcome to the drs_downloader."""
    print(cli.__doc__)
    return True


# @cli.command()
def config():
    """Configures the downloader."""
    return True


# @cli.command()
def credentials():
    """Authenticates the user."""
    return True


# @cli.command()
# @click.option('--file', default=None, show_default=True, help='Path containing the URIs to download.')
# @click.option('--dest', default=None, show_default=True, help='Path containing the URIs to download.')
def download(credentials_path: Path, file: Path, dest: Path) -> None:
    """Downloads the DRS object."""
    # The DRS URIs
    # A Google project to be billed for the download costs
    # A download destination
    # User credentials
    endpoint = 'https://development.aced-idp.org'
    drs_api = '/ga4gh/drs/v1/objects/'

    credentials = bdcDRSClient(credentials_path, 's3', debug=True),
    urls = []
    # Read URI's from provided file
    with open(file, 'r') as uris_file:
        uris = uris_file.read().splitlines()
        for uri in uris:
            # Create new DownloadURL instance and add it to the list for the async downloader.
            id = uri.split(':')[-1]
            download_url = get_signed_url(credentials, id)
            find_data_url = endpoint + drs_api + id
            drs = _send_request(find_data_url)
            md5 = drs['checksums'][0]['checksum']
            size = drs['size']
            print("Download URL: ", download_url, " md5 ", md5, " size ", size)

            download_url = async_downloader.DownloadURL(
                download_url, md5, size)
            urls.append(download_url)

    # Start download of the given URI.
    async_downloader.download(urls, dest)


# @cli.command()
# @click.option('--uri', default=None, show_default=True, help='URI of the file of interest')
def info(uri: str) -> dict:
    """Displays information of a given DRS object."""

    response = {}
    # Send request to endpoint with the DRS object URI provided by the user.
    url = endpoint + drs_api + uri
    try:
        response = _send_request(url)

    except Exception:
        print("response call to _send_request(url) has failed in info function")

    return response


# @cli.command()
# @click.option('--head', default=None, show_default=True, help='The first number of lines to display.')
# @click.option('--tail', default=None, show_default=True, help='The last last number of lines to display.')
# @click.option('--offset', default=None, show_default=True, help='The number of lines to skip before displaying.')
def list(head: int, tail: int, offset: int) -> None:
    """Lists all DRS Objects at a given endpoint."""
    try:
        url = endpoint + drs_api
        response = _send_request(url)
        drs_objects = response['drs_objects']
        ids = []
        for drs_object in drs_objects:
            id = drs_object['id']
            ids.append(id)

        if (head == None and tail == None):
            print(ids)
            return
    except Exception:
        print("List function Drs_objects Exceptions has occured")

    if ((head != None and type(head != int)) or (tail != None and type(tail) != int)):
        print("Please only enter numbers for head and tail and only strings of format 'int int' for indices")
        raise Exception

    if (head != None and tail != None):
        print("cannot handle that option, try only head or only tail flags")
        raise Exception

    # (indices != None and indices > len(ids)):
    if (head != None and head > len(ids)) or (tail != None and tail > len(ids)):
        print("head or tail value is greater than the number of IDs in the database which is ", len(ids))
        raise Exception

    if (head != None):
        print(ids[0:head])
        return

    if (tail != None):
        print(ids[-tail:-1])
        return


def _send_request(url: str) -> dict:
    # If no errors return JSON, otherwise print response status code.
    json_resp = {}
    try:
        response = requests.get(url)
        response.raise_for_status()
        resp = response.content.decode('utf-8')
        json_resp = json.loads(resp)

    except Exception:
        print("exception has occured in _send_request")

    return json_resp


# @cli.command()
# @click.option('--ids', default=None, show_default=True, help='The ')
# @click.option('--credentials', default=None, show_default=True, help='The first number of lines to display.')
def get_signed_url(credentials: bdcDRSClient, drs_ids: List[str]) -> str:

    test_data = {
        'BioDataCatalyst': {
            'drs_client': credentials,
            'drs_ids': [drs_ids]
        }
    }

    print(test_data)
    url = ''
    for (testname, test) in test_data.items():
        print(testname)
        drsClient = test['drs_client']

        for drs_id in test['drs_ids']:
            res = drsClient.get_object(drs_id)
            #print(f'GetObject for {drs_id}')
            #print (json.dumps(res, indent=3))
            # Get and access URL
            try:
                url = drsClient.get_access_url(drs_id)
                #print(f'URL for {drs_id}')
                #print (url)
                return url
            except:
                if drsClient.api_key == None:
                    print(
                        "This DRS client has not obtained authorization and cannot obtain URLs for controlled access objects")
                else:
                    print("You may not have authorization for this object")

    return url


def _get_uris(url: str) -> List[str]:
    """Helper method to write DRS id's from a given endpoint to a file 'uris.txt'. Useful for testing."""
    ids = []

    try:
        url = endpoint + drs_api
        response = _send_request(url)
        drs_objects = response['drs_objects']
        for drs_object in drs_objects:
            id = drs_object['self_uri']
            ids.append(id)

        with open('uris.txt', 'w') as uris:
            for id in ids:
                uris.write(id + '\n')
    except Exception:
        print("Exception occured in _get_uris")

    return ids


if __name__ == '__main__':
    cli()
