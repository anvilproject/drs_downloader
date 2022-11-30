from pathlib import Path
import tempfile
import pytest

from drs_downloader.upgrader import Upgrader


@pytest.mark.auth
def test_upgrader():
    with tempfile.TemporaryDirectory() as dest:
        upgrader = Upgrader()
        upgrader.upgrade(dest)

        download_file = Path(dest, 'drs_downloader')
        assert download_file.is_file()

        upgrader = Upgrader()
        upgrader.upgrade(dest)
        assert download_file.is_file()

        backup_dir = Path(dest, 'drs_downloader.bak')
        backup_file = Path(backup_dir, 'drs_downloader')

        assert backup_dir.is_dir()
        assert backup_file.is_file()
