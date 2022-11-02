import os
import tempfile
from drs_downloader.main import download_files


def test_download_files():
    with tempfile.TemporaryDirectory() as dest:
        download_files('tests/terra-data.tsv', 'pfb:ga4gh_drs_uri', dest)
        files = next(os.walk(dest))[2]
        assert len(files) == 10
