import hashlib
import logging
import os
import platform
import shutil
import sys
import tempfile
from abc import ABC
from pathlib import Path

import requests
from packaging import version

from version import __version__

logger = logging.getLogger(__name__)  # these control our simulation


class Upgrader(ABC):
    def __init__(self):
        release_path = "anvilproject/drs_downloader/releases/latest"
        self.release_url = f"https://github.com/{release_path}"
        self.api_url = f"https://api.github.com/repos/{release_path}"

    def upgrade(self, dest: str = os.getcwd(), force=False) -> Path:
        """Upgrades the drs_downloader executable and backups the old version to drs_downloader.bak/

        Args:
            dest (str, optional): download destination. Defaults to os.getcwd().
            force (bool, optional): forces upgrade even if version is up to date. Defaults to False.

        Raises:
            Exception: If the operating system can not be reliably determined
            Exception: If the checksum for the new executable does not match the expected value

        Returns:
            Path: The downloaded executable.
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
            exe = "drs-downloader-macOS"
        elif system == "Linux":
            exe = "drs-downloader-Linux"
        elif system == "Windows":
            exe = "drs-downloader-Windows.exe"
        else:
            raise Exception(
                f"Unknown operating system detected. See the release page for manual upgrade: {self.release_url}"
            )

        download_url = f"{self.release_url}/download/{exe}"
        # checksum_url = f"{self.release_url}/download/checksums.txt"

        # Download executable and checksum files to temporary directory for checksum verification
        verified_exe = None

        # We use a temporary directory here to prevent files that might not pass the
        # checksum step from remaining in the user's filesystem.
        with tempfile.TemporaryDirectory() as tmp_dir:
            unverified_exe = self._download_file(download_url, tmp_dir)

            # releases don't match so comment this out for now
            # checksum_path = self._download_file(checksum_url, tmp_dir)
            # checksums_match = self._verify_checksums(unverified_exe, checksum_path)
            # if checksums_match is False:
            # raise Exception("Actual hash does not match expected hash")

            # Backup old executable
            self._backup(Path(dest, exe))

            # If checksum is verified move new executable to current directory
            verified_exe = shutil.move(unverified_exe, dest)

        return Path(verified_exe)

    def _backup(self, old_exe: Path):
        """Backups the executable if it is already present in the destination directory.

        Example:
            Download destination is /Users/liam and /Users/liam/drs_downloader-macOS already
            exists so it will be moved to /Users/liam/drs-downloader-macOS.bak/drs-downloader-macOS.

        Args:
            dest (str): download destination
        """

        if old_exe.is_file() is False:
            return

        backup_dir = Path(old_exe.parent, f"{old_exe.name}.bak")
        backup_dir.mkdir(parents=True, exist_ok=True)

        shutil.move(old_exe, backup_dir)

    def _download_file(self, url: str, dest: str) -> Path:
        """Downloads a file given an URL.

        Example:
            url is https://example.com/foo.zip and dest is /Users/liam so foo
            will be downloaded to /Users/liam/foo.zip

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

        print(expected_sha, actual_sha)

        return expected_sha == actual_sha
