from pathlib import Path
from click.testing import CliRunner
from drs_downloader.cli import _extract_tsv_info, cli
from drs_downloader.clients.terra import TerraDrsClient
from drs_downloader.manager import DrsAsyncManager
import os
import pytest
import shutil
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


def test_check_existing_files():
    drs_manager, drs_objects = _get_drs_manager()

    with tempfile.TemporaryDirectory() as dest:
        filtered_objects = drs_manager.filter_existing_files(drs_objects, dest)
        assert len(filtered_objects) == len(drs_objects)

        shutil.copy2("tests/fixtures/parts/HG00536.final.cram.crai", dest)
        filtered_objects = drs_manager.filter_existing_files(drs_objects, dest)
        assert len(filtered_objects) == len(drs_objects) - 1


def test_existing_parts():
    _part_exists("HG00536.final.cram.crai.0.1048576.part")
    _part_exists("HG00536.final.cram.crai.1048577.1244278.part")


def _part_exists(part_file):
    part_fixture = Path("tests/fixtures/parts", part_file)
    start = int(part_fixture.name.split(".")[-3])
    size = int(part_fixture.name.split(".")[-2])
    drs_manager, _ = _get_drs_manager()

    with tempfile.TemporaryDirectory() as dest:
        part_path = Path(dest, part_fixture.name)
        part_exist = drs_manager.check_existing_parts(part_path, start, size)
        assert part_exist is False

        shutil.copy2(part_fixture, dest)
        part_exist = drs_manager.check_existing_parts(part_path, start, size)
        assert part_exist is True

        with open(part_path, 'w'):
            pass
        part_exist = drs_manager.check_existing_parts(part_path, start, size)
        assert part_exist is False


def test_extract_tsv_info():
    tsv_path = Path("tests/fixtures/terra-data.tsv")
    drs_header = "pfb:ga4gh_drs_uri"
    expected_len = 10
    expected_first = "drs://dg.4503:dg.4503/15fdd543-9875-4edf-8bc2-22985473dab6"
    expected_last = "drs://dg.4503:dg.4503/bf2b854a-17a3-4b3c-aeb2-4f670ceb9e85"

    # Pass in input manifest file
    uris = _extract_tsv_info(tsv_path, None)
    assert len(uris) == expected_len
    assert uris[0] == expected_first
    assert uris[9] == expected_last

    # Pass in input manifest file and DRS column header
    uris = _extract_tsv_info(tsv_path, drs_header)
    assert len(uris) == expected_len
    assert uris[0] == expected_first
    assert uris[9] == expected_last

    # Bad DRS column header
    with pytest.raises(KeyError):
        _extract_tsv_info(tsv_path, "Foobar")

    # No header found in manifest file
    with pytest.raises(KeyError):
        _extract_tsv_info(Path("tests/fixtures/no-header.tsv"), None)

    # No header found in manifest file despite user passing in header value
    with pytest.raises(KeyError):
        _extract_tsv_info(Path("tests/fixtures/no-header.tsv"), drs_header)


def _get_drs_manager():
    tsv_path = "tests/fixtures/manifests/terra-data.tsv"
    drs_header = "pfb:ga4gh_drs_uri"

    # get a drs client
    drs_client = TerraDrsClient()
    # assign it to a manager
    drs_manager = DrsAsyncManager(drs_client=drs_client)
    ids_from_manifest = _extract_tsv_info(tsv_path, drs_header)
    drs_objects = drs_manager.get_objects(ids_from_manifest)
    drs_objects.sort(key=lambda x: x.size, reverse=False)
    return drs_manager, drs_objects
