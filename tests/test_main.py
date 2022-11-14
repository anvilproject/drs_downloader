import os
import pytest
import tempfile
from drs_downloader.main import download_files, _extract_tsv_info


@pytest.mark.skip
def test_download_files():
    with tempfile.TemporaryDirectory() as dest:
        download_files('tests/terra-data.tsv', 'pfb:ga4gh_drs_uri', dest)
        files = next(os.walk(dest))[2]
        assert len(files) == 10


def test_extract_tsv_info():
    tsv_path = 'tests/terra-data.tsv'
    drs_header = 'pfb:ga4gh_drs_uri'
    uris = _extract_tsv_info(tsv_path, drs_header)
    assert len(uris) == 10

    assert uris[0] == 'drs://dg.4503:dg.4503/15fdd543-9875-4edf-8bc2-22985473dab6'
    assert uris[9] == 'drs://dg.4503:dg.4503/bf2b854a-17a3-4b3c-aeb2-4f670ceb9e85'

    with pytest.raises(KeyError):
        _extract_tsv_info(tsv_path, 'Foobar')

    with pytest.raises(KeyError):
        _extract_tsv_info('tests/no-header.tsv', drs_header)
