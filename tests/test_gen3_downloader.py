import os
import tempfile
from typing import List
from drs_downloader.gen3_downloader import download_drs, _get_signed_url, _create_download_url, \
    _extract_tsv_info, _send_request

expectedUrl = 'https://minio-default.aced-idp.org/aced-default/FOOBAR%3Ad62af3ff-756d-4e73-b6c6-e0bbd3bae50d/' \
    'tests/fixtures/projects/MyFirstProject/DATA/0940858111'


def test_download_drs():
    with tempfile.TemporaryDirectory() as dest:
        download_drs('tests/gen3-data.tsv', dest)
        files = next(os.walk(dest))[2]
        assert len(files) == 10


def test_get_signed_url():
    object_url = 'https://development.aced-idp.org/ga4gh/drs/v1/objects/d62af3ff-756d-4e73-b6c6-e0bbd3bae50d'
    signedUrl = _get_signed_url(object_url)
    assert signedUrl.startswith(expectedUrl)


def test_create_download_url():
    uri = 'drs://FOOBAR:d62af3ff-756d-4e73-b6c6-e0bbd3bae50d'
    downloadUrl = _create_download_url(uri)
    assert downloadUrl
    assert downloadUrl.md5 == 'f06595beebde900ab8ffce129bbed271'
    assert downloadUrl.size == 3975
    assert downloadUrl.url.startswith(expectedUrl)


def test_send_request():
    url = 'https://development.aced-idp.org/ga4gh/drs/v1/objects/'
    response = _send_request(url)
    assert len(response['drs_objects']) == 51


def test_extract_tsv_info():
    tsv_file = 'tests/gen3-data.tsv'
    uris = _extract_tsv_info(tsv_file)
    assert len(uris) == 10
    assert uris[0].startswith('drs://FOOBAR:')


def _get_uris(url: str) -> List[str]:
    response = _send_request(url)
    uris = []
    for drs_object in response['drs_objects']:
        uri = drs_object['self_uri']
        uris.append(uri)

    return uris


def _write_data_file():
    endpoint = 'https://development.aced-idp.org/ga4gh/drs/v1/objects?page=10'
    uris = _get_uris(endpoint)
    with open('tests/gen3-data.tsv', 'w') as file:
        file.write('file_drs_uri' + '\n')
        for uri in uris:
            file.write(uri + '\n')


# if __name__ == '__main__':
#     _write_data_file()
