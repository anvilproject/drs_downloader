import hashlib
import logging
import os
import platform
import sys
import tempfile
from abc import ABC
from pathlib import Path
from zipfile import ZipFile

import requests
from packaging import version

from version import __version__

logger = logging.getLogger(__name__)  # these control our simulation


class Upgrader(ABC):
    def __init__(self):
        release_path = "anvilproject/drs_downloader/releases/latest"
        self.release_url = f"https://github.com/{release_path}"
        self.api_url = f"https://api.github.com/repos/{release_path}"

    def upgrade(self, dest: str = os.getcwd(), force=False):
        """Upgrades the drs_downloader executable and backups the old version to drs_downloader.bak/

        Args:
            dest (str, optional): download destination. Defaults to os.getcwd().
            force (bool, optional): forces upgrade even if version is up to date. Defaults to False.

        Raises:
            Exception: If the operating system can not be reliably determined
            Exception: If the checksum for the new executable does not match the expected value
        """

        # Perform upgrade only if the program is being run as an executable and not as a script
        if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
            logger.info("running in a PyInstaller bundle")
        else:
            logger.info("Running from a script")

        # Upgrade if newer version is available
        json = requests.get(self.api_url).json()
        new_version = version.parse(json["tag_name"])
        current_version = version.parse(__version__)

        if current_version >= new_version:
            logger.info("Latest version already installed")
            if force is False:
                return

        # Determine download url for operating system
        system = platform.system()
        if system == "Darwin":
            zip = "drs-downloader-macOS.zip"
        elif system == "Linux":
            zip = "drs-downloader-Linux.zip"
        elif system == "Windows":
            zip = "drs-downloader-Windows.zip"
        else:
            raise Exception(
                f"Unknown operating system detected. See the release page for manual upgrade: {self.release_url}"
            )

        download_url = f"{self.release_url}/download/{zip}"
        checksum_url = f"{self.release_url}/download/checksums.txt"

        # Download zip and checksum files to temporary directory for checksum verification
        zip_path = None
        checksum_path = None

        with tempfile.TemporaryDirectory() as tmp_dir:
            zip_path = self._download_file(download_url, tmp_dir)
            checksum_path = self._download_file(checksum_url, tmp_dir)

            checksums_match = self._verify_checksums(zip_path, checksum_path)
            if checksums_match is False:
                raise Exception("Actual hash does not match expected hash")

            # Backup old executable
            self._backup(dest)

            # Move new executable to current directory
            with ZipFile(zip_path, "r") as zip_file:
                zip_file.extractall(path=Path(dest))

    def _backup(self, dest: str):
        """Backups the executable if it is already present in the destination directory.

        Example:
            Download destination is /home/liam and /home/liam/drs_downloader already
            exists so it will be moved to /home/liam/drs_downloader.bak/drs_downloader.

        Args:
            dest (str): download destination
        """

        name = "drs_downloader"
        download_file = Path(dest, name)
        if download_file.is_file() is False:
            return

        backup_dir = Path(dest, f"{name}.bak")
        backup_dir.mkdir(parents=True, exist_ok=True)

        old_executable = Path(dest, name)
        old_executable.rename(Path(backup_dir, name))

    def _download_file(self, url: str, dest: str) -> Path:
        """Downloads a file given an URL.

        Example:
            url is https://example.com/foo.zip and dest is /home/liam so foo
            will be downloaded to /home/liam/foo.zip

        Args:
            url (str): URL to request the file from
            dest (str): download destination

        Returns:
            Path: path of the downloaded file
        """

        response = requests.get(url)
        file_name = url.split("/")[-1]
        path = Path(dest, file_name)
        with open(path, "wb") as f:
            f.write(response.content)

        return path

    def _verify_checksums(self, file: str, checksums: str) -> bool:
        """Compares checksums for a given file against those in a given list (typically checksums.txt)

        Args:
            file (str): File to verify checksums for
            checksums (str): File containing checksums

        Returns:
            bool: True if the expected and actual checksums match, False otherwise
        """

        expected_sha = ""
        with open(checksums, "r") as checksum_file:
            lines = checksum_file.readlines()
            for line in lines:
                # If filename is found then use that checksum as the expected value
                if file.stem in line:
                    expected_sha = line.split()[0]

        # Verify checksums
        sha_hash = hashlib.sha256()
        sha_hash.update(open(file, "rb").read())
        actual_sha = sha_hash.hexdigest()

        return expected_sha == actual_sha
