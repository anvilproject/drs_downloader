from pathlib import Path

import pytest

from drs_downloader.cli import _extract_tsv_info


def test_extract_tsv_info():
    tsv_path = Path("tests/fixtures/terra-data.tsv")
    drs_header = "pfb:drs_uri"
    expected_len = 9
    expected_first = "drs://drs.anv0:v2_68763021-a85b-3bfb-9c46-9b038d66f75e"
    expected_last = "drs://drs.anv0:v2_1f00c969-b51b-33a0-b71f-dd6fbf0888fb"

    # Pass in input TSV file
    uris = _extract_tsv_info(tsv_path, None)
    assert len(uris) == expected_len
    assert uris[0] == expected_first
    assert uris[8] == expected_last

    # Pass in input TSV file and DRS column header
    uris = _extract_tsv_info(tsv_path, drs_header)
    assert len(uris) == expected_len
    assert uris[0] == expected_first
    assert uris[8] == expected_last

    # Bad DRS column header
    with pytest.raises(KeyError):
        _extract_tsv_info(tsv_path, "Foobar")

    # No header found in TSV file
    with pytest.raises(KeyError):
        _extract_tsv_info(Path("tests/fixtures/no-header.tsv"), None)

    # No header found in TSV file despite user passing in header value
    with pytest.raises(KeyError):
        _extract_tsv_info(Path("tests/fixtures/no-header.tsv"), drs_header)
