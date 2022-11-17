from drs_downloader.main import download_files, _extract_tsv_info, _sign_urls
import os
import pytest
import tempfile


@pytest.mark.skip
def test_download_files():
    with tempfile.TemporaryDirectory() as dest:
        download_files("tests/terra-data.tsv", dest)
        files = sorted(next(os.walk(dest))[2])
        assert len(files) == 10
        assert files[0] == "HG00536.final.cram.crai"
        assert files[9] == "NA20525.final.cram.crai"


@pytest.mark.skip
def test_sign_urls():
    expected_url = (
        "https://nih-nhlbi-biodata-catalyst-1000-genomes.storage.googleapis.com/CCDG_13607/Project_C"
        "CDG_13607_B01_GRM_WGS.cram.2019-02-06/Sample_HG00536/analysis/HG00536.final.cram.crai"
    )
    expected_md5 = "750004f80de56fb9e74bde5a252b0260"
    expected_size = 1244278

    uris = _extract_tsv_info("tests/terra-data.tsv")
    urls = sorted(_sign_urls(uris, 10), key=lambda signed_url: signed_url.url)
    assert len(urls) == 10
    # The key value pairs change for every signing so only compare the part of the signed URL that doesn't change
    assert urls[0].url.startswith(expected_url)
    assert urls[0].md5 == expected_md5
    assert urls[0].size == expected_size


def test_extract_tsv_info():
    tsv_path = "tests/terra-data.tsv"
    drs_header = "pfb:ga4gh_drs_uri"
    expected_len = 10
    expected_first = "drs://dg.4503:dg.4503/15fdd543-9875-4edf-8bc2-22985473dab6"
    expected_last = "drs://dg.4503:dg.4503/bf2b854a-17a3-4b3c-aeb2-4f670ceb9e85"

    # Pass in input TSV file
    uris = _extract_tsv_info(tsv_path)
    assert len(uris) == expected_len
    assert uris[0] == expected_first
    assert uris[9] == expected_last

    # Pass in input TSV file and DRS column header
    uris = _extract_tsv_info(tsv_path, drs_header)
    assert len(uris) == expected_len
    assert uris[0] == expected_first
    assert uris[9] == expected_last

    # Bad DRS column header
    with pytest.raises(ValueError):
        _extract_tsv_info(tsv_path, "Foobar")

    # No header found in TSV file
    with pytest.raises(ValueError):
        _extract_tsv_info("tests/no-header.tsv")

    # No header found in TSV file despite user passing in header value
    with pytest.raises(ValueError):
        _extract_tsv_info("tests/no-header.tsv", drs_header)
