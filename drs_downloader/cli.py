from re import I
import click
import requests
import json
import async_downloader.download as async_downloader
from pathlib import Path

endpoint = 'https://development.aced-idp.org'
drs_api = '/ga4gh/drs/v1/objects/'


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
def download(file, dest):
    """Downloads the DRS object."""
    # The DRS URIs
    # A Google project to be billed for the download costs
    # A download destination
    # User credentials
    try:
        with open(file, 'r') as uris_file:
            uris = uris_file.readlines()
        urls = _uris_to_urls(uris)

    urls = []

    # Read URI's from provided file
    with open(file, 'r') as uris_file:
        uris = uris_file.read().splitlines()
        for uri in uris:
            # Create new DownloadURL instance and add it to the list for the async downloader.
            download_url = _createDownloadUrl(uri)
            urls.append(download_url)

    # Start download of the given URI.
    async_downloader.download(urls, Path('DATA'))


def _createDownloadUrl(uri):
    # Get md5 hash and size of the file to create a DownloadURL instance.
    url = _uri_to_url(uri)
    drs = _send_request(url)
    md5 = drs['checksums'][0]['checksum']
    size = drs['size']

    download_url = async_downloader.DownloadURL(url, md5, size)
    return download_url

# @cli.command()
# @click.option('--uri', default=None, show_default=True, help='URI of the file of interest')
def info(uri):
    """Displays information of a given DRS object."""

    # Send request to endpoint with the DRS object URI provided by the user.
    assert(uri,endpoint,drs_api)
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
def list(head, tail, offset):
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
    


    if ( (head != None  and type(head != int)) or (tail != None and type(tail) != int)):
        print("Please only enter numbers for head and tail and only strings of format 'int int' for indices")
        raise Exception

    if (head != None and tail != None):
        print("cannot handle that option, try only head or only tail flags")
        raise Exception
    
    if (head != None and head > len(ids)) or (tail != None and tail > len(ids)):  # (indices != None and indices > len(ids)):
        print("head or tail value is greater than the number of IDs in the database which is ", len(ids))
        raise Exception

    if (head != None):
        print(ids[0:head])
        return

    if (tail != None):
        print(ids[-tail:-1])
        return

def _send_request(url):
    # If no errors return JSON, otherwise print response status code.
    try:
        response = requests.get(url)
        response.raise_for_status()
        resp = response.content.decode('utf-8')
        json_resp = json.loads(resp)
        return json_resp

    except Exception:
        print("exception has occured in _send_request")
    


# @cli.command()
def signed_url():
    """Downloads a DRS object from a signed URL."""
    return True


def _uris_to_urls(uris):
    try:
        urls = []
        for uri in uris:
            id = uri.split(':')[-1]
            url = endpoint + drs_api + id
            urls.append(url)
            return urls

    except Exception:
        print("a general uris_to_urls Exception has occurred")




def _get_uris():
    try:
        url = endpoint + drs_api
        response = _send_request(url)
        drs_objects = response['drs_objects']
        assert(drs_objects)
        ids = []
        for drs_object in drs_objects:
            id = drs_object['self_uri']
            assert(id)
            ids.append(id)

        with open('uris.txt', 'w') as uris:
            for id in ids:
                uris.write(id + '\n')
    except Exception:
        print("Exception occured in _get_uris")


if __name__ == '__main__':
    # cli()
    download('uris.txt', '/tmp')
    # _get_uris()
