from tests import MANIFESTS, PARTS, INTERRUPTED_DOWNLOAD, COMPLETE_DOWNLOAD
from drs_downloader.manager import DrsAsyncManager
from drs_downloader.clients.terra import TerraDrsClient
from drs_downloader.cli import _extract_tsv_info, cli
from click.testing import CliRunner
import filecmp
import logging
import os
import shutil
import tempfile
from pathlib import Path
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))


def test_interrupted_download(caplog):
    """Tests recovering from an interrupted download.

    To simulate an incomplete download a drs_downloader process was terminated in the middle of downloading using
    terra-data.tsv as the manifest file. The files in the destination were copied into the interrupted_download
    directory within tests/fixtures.

    In the interrupted download directory there are:
        - 9 total DRS objects
        - 1 complete DRS object CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.
        cram.2019-02-06_Sample_HG02982_analysis_HG02982.final.cram.crai
        - 8 incomplete DRS objects
    """

    caplog.set_level(logging.INFO)
    complete_files = sorted(next(os.walk(COMPLETE_DOWNLOAD))[2])
    assert (
        len(complete_files) == 9
    )  # asserting that the fixtures have the expected number of files

    with tempfile.TemporaryDirectory() as dest:
        for file in os.listdir(INTERRUPTED_DOWNLOAD):
            shutil.copy2(INTERRUPTED_DOWNLOAD / file, dest)

        assert _are_dirs_equal(COMPLETE_DOWNLOAD, dest) is False

        runner = CliRunner()
        result = runner.invoke(
            cli, ["terra", "-d", dest, "-m", MANIFESTS / "terra-data.tsv"]
        )
        assert result.exit_code == 0

        assert _are_dirs_equal(COMPLETE_DOWNLOAD, dest) is True

        # assert f"HG04209.final.cram.crai already exists in {dest}. Skipping download." in caplog.messages
        # assert "HG00536.final.cram.crai had 1 existing parts." in caplog.messages

        # Run the downloader again in the destination directory to verify that all DRS objects are present
        result = runner.invoke(
            cli, ["terra", "-d", dest, "-m", MANIFESTS / "terra-data.tsv"]
        )
        assert result.exit_code == 0
        assert f"All DRS objects already present in {dest}." in caplog.messages


def _are_dirs_equal(dir1: str, dir2: str) -> bool:
    """Compares all files between two directories

    Args:
        dir1 (str): The first directory to compare (e.g. "expected" values)
        dir2 (str): The second directory to compare (e.g. "actual" values)

    Returns:
        bool: True if the directories contain the same files, False otherwise
    """

    dir1_files = sorted(next(os.walk(dir1))[2])
    dir2_files = sorted(next(os.walk(dir2))[2])
    if len(dir1_files) != len(dir2_files):
        return False

    dirs_cmp = filecmp.dircmp(dir1, dir2)

    if len(dirs_cmp.left_only) > 0 or len(dirs_cmp.right_only) > 0:
        return False

    (_, mismatch, errors) = filecmp.cmpfiles(dir1, dir2, dirs_cmp.common_files)

    if len(mismatch) > 0 or len(errors) > 0:
        return False

    return True


def test_check_existing_files():
    drs_manager, drs_objects = _get_drs_manager()

    with tempfile.TemporaryDirectory() as dest:
        replace = False
        filtered_objects = drs_manager.filter_existing_files(drs_objects, dest, replace, verbose=True)
        assert len(filtered_objects) == len(drs_objects)

        shutil.copy2(PARTS / "CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.\
2019-02-06_Sample_HG02982_analysis_HG02982.final.cram.crai", dest)
        filtered_objects = drs_manager.filter_existing_files(drs_objects, dest, replace, verbose=True)
        assert len(filtered_objects) == len(drs_objects) - 1

    # Part files that have been completely downloaded
    complete_parts = []

    # Part files that were incompletely downloaded when the download terminated
    incomplete_parts = [
        "CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.\
2019-02-06_Sample_HG00188_analysis_HG00188.final.cram.crai.0.1048576.part",
        "CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.\
2019-02-06_Sample_HG01923_analysis_HG01923.final.cram.crai.0.1048576.part",
        "CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.\
2019-02-06_Sample_HG02315_analysis_HG02315.final.cram.crai.0.1048576.part",
        "CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.\
2019-02-06_Sample_HG02840_analysis_HG02840.final.cram.crai.0.1048576.part",
        "CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.\
2019-02-06_Sample_NA11918_analysis_NA11918.final.cram.crai.0.1048576.part",
        "CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.\
2019-02-06_Sample_NA19062_analysis_NA19062.final.cram.crai.0.1048576.part",
        "CCDG_14151_Project_CCDG_14151_B01_GRM_WGS.cram.\
2020-02-12_Sample_HG00639_analysis_HG00639.final.cram.crai.0.1048576.part",
        "CCDG_14151_Project_CCDG_14151_B01_GRM_WGS.cram.\
2020-02-12_Sample_NA12344_analysis_NA12344.final.cram.crai.0.1048576.part",
        "CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.\
2019-02-06_Sample_HG00188_analysis_HG00188.final.cram.crai.1048577.1267310.part",
        "CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.\
2019-02-06_Sample_HG01923_analysis_HG01923.final.cram.crai.1048577.1267264.part",
        "CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.\
2019-02-06_Sample_HG02315_analysis_HG02315.final.cram.crai.1048577.1267655.part",
        "CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.\
2019-02-06_Sample_HG02840_analysis_HG02840.final.cram.crai.1048577.1267330.part",
        "CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.\
2019-02-06_Sample_NA11918_analysis_NA11918.final.cram.crai.1048577.1267432.part",
        "CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.\
2019-02-06_Sample_NA19062_analysis_NA19062.final.cram.crai.1048577.1267131.part",
        "CCDG_14151_Project_CCDG_14151_B01_GRM_WGS.cram.\
2020-02-12_Sample_HG00639_analysis_HG00639.final.cram.crai.1048577.1267744.part",
        "CCDG_14151_Project_CCDG_14151_B01_GRM_WGS.cram.\
2020-02-12_Sample_NA12344_analysis_NA12344.final.cram.crai.1048577.1267830.part"
    ]

    with tempfile.TemporaryDirectory() as dest:
        for part in complete_parts:
            assert _part_exists(part, dest) is True

        for part in incomplete_parts:
            assert _part_exists(part, dest) is False


def _part_exists(part_file: str, dest: str) -> bool:
    part_fixture = INTERRUPTED_DOWNLOAD / part_file
    shutil.copy2(part_fixture, dest)

    start = int(part_fixture.name.split(".")[-3])
    size = int(part_fixture.name.split(".")[-2])
    drs_manager, _ = _get_drs_manager()

    part_path = Path(dest, part_file)
    print("start: ", start, "size: ", size)
    return drs_manager.check_existing_parts(part_path, start, size, verbose=True)


def _get_drs_manager():
    tsv_path = MANIFESTS / "terra-data.tsv"
    drs_header = "pfb:drs_uri"

    # get a drs client
    drs_client = TerraDrsClient()
    # assign it to a manager
    drs_manager = DrsAsyncManager(drs_client=drs_client)
    ids_from_manifest = _extract_tsv_info(Path(tsv_path), drs_header)
    drs_objects = drs_manager.get_objects(ids_from_manifest, verbose=True)
    drs_objects.sort(key=lambda x: x.size, reverse=False)
    return drs_manager, drs_objects
