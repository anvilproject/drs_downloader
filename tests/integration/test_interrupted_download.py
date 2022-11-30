from pathlib import Path
from click.testing import CliRunner
from drs_downloader.cli import cli
import filecmp
import os
import pytest
import shutil
import tempfile


@pytest.mark.auth
def test_interrupted_download():
    """Tests recovering from an interrupted download.
    In the download destination there are:
        - 10 DRS objects in total over 12 downloaded files
        - 1 complete download
        - 9 incomplete downloads
    """
    fixtures_dir = 'tests/fixtures'
    interrupted_dir = Path(fixtures_dir, 'interrupted_download')
    complete_dir = Path(fixtures_dir, 'complete_download')
    complete_files = sorted(next(os.walk(complete_dir))[2])
    assert len(complete_files) == 10

    with tempfile.TemporaryDirectory() as dest:
        for file in os.listdir(Path(interrupted_dir)):
            shutil.copy2(Path(interrupted_dir, file), dest)

        assert _are_dirs_equal(complete_dir, dest) is False

        runner = CliRunner()
        result = runner.invoke(cli, ['terra', '-d', dest])
        assert result.exit_code == 0

        assert _are_dirs_equal(complete_dir, dest) is True


def _are_dirs_equal(dir1, dir2):
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
