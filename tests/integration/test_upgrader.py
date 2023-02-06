import tempfile
from pathlib import Path

from drs_downloader.upgrader import Upgrader


def test_upgrader():
    with tempfile.TemporaryDirectory() as dest:
        upgrader = Upgrader()
        exe = upgrader.upgrade(dest, force=True)

        assert exe.is_file()

        exe = upgrader.upgrade(dest, force=True)
        assert exe.is_file()

        backup_dir = Path(exe.parent, f"{exe.name}.bak")
        backup_file = Path(backup_dir, exe.name)

        assert backup_dir.is_dir()
        assert backup_file.is_file()
