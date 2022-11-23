from click.testing import CliRunner
from drs_downloader.cli import _extract_tsv_info, cli
import os
import pytest
import tempfile


def test_terra():
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(cli, ['terra', '-d', dest])
        assert result.exit_code == 0

        files = sorted(next(os.walk(dest))[2])
        assert len(files) == 10
        assert files[0] == "HG00536.final.cram.crai"
        assert files[9] == "NA20525.final.cram.crai"


def test_extract_tsv_info():
    tsv_path = "tests/fixtures/terra-data.tsv"
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
        _extract_tsv_info("tests/fixtures/no-header.tsv", None)

    # No header found in TSV file despite user passing in header value
    with pytest.raises(KeyError):
        _extract_tsv_info("tests/fixtures/no-header.tsv", drs_header)
