from pathlib import Path

import pytest

from drs_downloader.cli import _extract_tsv_info
from tests import MANIFESTS


def test_extract_tsv_info():
    tsv_path = Path(MANIFESTS, "terra-data.tsv")
    drs_header = "pfb:ga4gh_drs_uri"
    expected_len = 10
    expected_first = "drs://dg.4503:dg.4503/15fdd543-9875-4edf-8bc2-22985473dab6"
    expected_last = "drs://dg.4503:dg.4503/bf2b854a-17a3-4b3c-aeb2-4f670ceb9e85"

    # Pass in input TSV file
    uris = _extract_tsv_info(tsv_path, None)
    assert len(uris) == expected_len
    assert uris[0] == expected_first
    assert uris[9] == expected_last

    # Pass in input TSV file and DRS column header
    uris = _extract_tsv_info(tsv_path, drs_header)
    assert len(uris) == expected_len
    assert uris[0] == expected_first
    assert uris[9] == expected_last

    # Bad DRS column header
    with pytest.raises(KeyError):
        _extract_tsv_info(tsv_path, "Foobar")

    # No header found in TSV file
    with pytest.raises(KeyError):
        _extract_tsv_info(Path(MANIFESTS, "no-header.tsv"), None)

    # No header found in TSV file despite user passing in header value
    with pytest.raises(KeyError):
        _extract_tsv_info(Path(MANIFESTS, "no-header.tsv"), drs_header)
