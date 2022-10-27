import click
import requests
import json
import async_downloader.download as async_downloader
from pathlib import Path
import os.path
from DRSClient import DRSClient
import pandas as pd
import aiohttp
import asyncio
import time
import logging 

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
    '''Handles Gen3 specific authentication using Fence'''
    
    # Initialize a DRS Client for the service at the specified url base
    # and with the REST resource to provide an access key 
    def __init__(self, api_url_base,  access_token_resource_path, api_key_path,
    access_id=None, debug=False):
        super().__init__(api_url_base, access_id, debug=debug)
        self.api_key = None
        self.access_token_resource_path = access_token_resource_path
        self.api_key_path = api_key_path
        #self.authorize()

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
                print('Error {} getting Access token for {}'.format(code, self.api_url_base))
                print('Using {}'.format(full_key_path))
                self.api_key = None
                
        except:
            self.api_key = None

    # Obtain an access_token using the provided Fence API key.
    # The client object will retain the access key for subsequent calls
    def updateAccessToken(self):
        headers = {'Content-Type': 'application/json'}
        api_url = '{0}{1}'.format(self.api_url_base, self.access_token_resource_path)
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
        super().__init__('https://development.aced-idp.org', '/user/credentials/cdis/access_token',api_key_path, access_id, debug)


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

async def get_md5_and_size(session, url):
    async with session.get(url) as resp:
        md5_and_size = await resp.json()
        md5 = md5_and_size['checksums'][0]['checksum']
        size = md5_and_size['size']
        return md5, size

async def get_md5s_and_sizes(file):
    endpoint = 'https://development.aced-idp.org'
    drs_api = '/ga4gh/drs/v1/objects/'

    with open(file, 'r') as uris_file:
        uris = uris_file.read().splitlines()
        whats_this = map(lambda x: endpoint + drs_api + x.split(':')[-1], uris)
        results = [list_obj for list_obj in whats_this]

    async with aiohttp.ClientSession() as session:
        tasks = []
        for result in results:
            url = tasks.append(asyncio.ensure_future(get_md5_and_size(session, result)))
        md5s_and_sizes = await asyncio.gather(*tasks)
        return results, md5s_and_sizes
        
def download(credentials_path,file, dest):
        urls = []
        start_1 = time.time()
        drs_client = bdcDRSClient(credentials_path, 's3', debug=True)
        uris, md5sandSizes= asyncio.run(get_md5s_and_sizes(file))
        end_1 = time.time()
        print('total time to do id parsing , and md5 and size http requests',end_1-start_1)
        start_2 = time.time()
        download_url = get_signed_url(drs_client,uris)
        end_2 = time.time() 
        print('total time to do the URL signing and authentication',end_2-start_2)
        start_3 = time.time()
        for md5andSize in md5sandSizes:
            download_url_bundle = async_downloader.DownloadURL(download_url[md5sandSizes.index(md5andSize)],md5andSize[0], md5andSize[1])
            urls.append(download_url_bundle)
        
        async_downloader.download(urls, Path('DATA'))
        end_3 = time.time()
        print("totla time to do the downlaodign and bundling of the information",end_3-start_3)
def get_signed_url(drs_client,Drs_ids):
    test_data = {'BioDataCatalyst' : {'drs_client': drs_client,'drs_ids': Drs_ids}}
    for (testname, test) in test_data.items():
        drsClient = test['drs_client']
        urls = []
        for drs_id in test['drs_ids']:
            url = drsClient.get_access_url(drs_id)
            #print(url)
            urls.append(url)
            
        return urls

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
# @click.option('--uri', default=None, show_default=True, help='URI of the file of interest')
def info(uri):
    """Displays information of a given DRS object."""

    # Send request to endpoint with the DRS object URI provided by the user.
    url = endpoint + drs_api + uri
    try:
        response = _send_request(url)
        return response

    except Exception:
        print("response call to _send_request(url) has failed in info function")


# @cli.command()
# @click.option('--head', default=None, show_default=True, help='The first number of lines to display.')
# @click.option('--tail', default=None, show_default=True, help='The last last number of lines to display.')
# @click.option('--offset', default=None, show_default=True, help='The number of lines to skip before displaying.')
def list():
    count =0
    """Lists all DRS Objects at a given endpoint."""
    num =0
    drs_api='/ga4gh/drs/v1/objects?page='+str(num)
    url = endpoint + drs_api
    response = _send_request(url)
    drs_objects = response['drs_objects']
    ids = []
    while(drs_objects):
        for drs_object in drs_objects:
            count=count+1
            id = drs_object['id']
            #print(id)
            if(drs_object and drs_object['access_methods'] and drs_object['access_methods'][0]['access_id']):
                ids.append(id)
        
        num=num+1
        drs_api='/ga4gh/drs/v1/objects?page='+str(num)
        url = endpoint + drs_api
        response = _send_request(url)
        drs_objects = response['drs_objects']
    #write URIS too why not
    #with open('uris.txt', 'w') as uris:
     #   for id in ids:
      #      uris.write(id + '\n')
    
    print(ids)
    #return ids
        
def Extract_TSV_Information(Tsv_Path):
    urls = []
    df = pd.read_csv(Tsv_Path,sep = '\t')
    for i in range(5):
        print(df['file_sha256'][i])
        print(df['file_size'][i])
        print(df['file_url'][i])
        download_url = async_downloader.DownloadURL(df['file_url'][i], df['file_sha256'][i], df['file_size'][i])
        urls.append(download_url)

    # Start download of the given URI.
    async_downloader.download(urls, Path('TSV_DATA'))

if __name__ == '__main__':
    #list()
    #Extract_TSV_Information('Terra Data - 1M Neurons 2022-10-25 03.36.tsv')
    start = time.time()
    download('~/Desktop/credentials.json','uris.txt','')
    end = time.time()

    print('Total Execution time',end-start)

    # cli()
    # _get_uris()


