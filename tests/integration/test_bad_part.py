import logging
import os
import shutil
import tempfile
from pathlib import Path

from click.testing import CliRunner

from drs_downloader.cli import cli


def test_bad_part(caplog):
    caplog.set_level(logging.INFO)

    with tempfile.TemporaryDirectory() as dest:
        for file in os.listdir(Path("tests/fixtures/interrupted_download")):
            shutil.copy2(Path(f"tests/fixtures/interrupted_download/{file}"), dest)

        bad_part = Path(dest, "CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.2019-02-06\
_Sample_HG00188_analysis_HG00188.final.cram.crai.0.1048576.part")

        # Overwrite the first few bytes of the part file with a string representing malformed or new data.
        # This will allow the bad part file to pass the filesize check but not the checksum validation.
        with open(bad_part, "r+b") as f:
            f.seek(0)
            f.write(b"bad data")

        runner = CliRunner()
        _ = runner.invoke(
            cli,
            ["terra", "-d", dest, "--manifest-path", "tests/fixtures/terra-data.tsv", "-v"],
        )
        # if the part sizes don't match it just gets redownloaded and returns a 0
        assert _.exit_code == 0

        print("THE VALUE OF MESSAGES", caplog.messages)
        assert (
            "DOES NOT MATCH" in str(caplog.messages)
        )

        """
         Assert that the part file passed the filesize check
         Although this feature is good in practice it is really unnecesary to the end user when a
        resign happens and this message is displayed 150 times
        assert (
            'HG00622.final.cram.crai.0.1048576.part exists and has expected size. Skipping download.'
            in messages
        )


         Assert that the file with the bad part was caught in the checksum step
         It never makes it here because the size if different than what was expected
         the warning message is old
        assert (
            "Actual md5 hash 9b507ccc2a8abb463e6ba128d9c957c5 does not match expected 238eb9fce97703ae1b9b6b6aaa00b0f3"
            in str(caplog.messages)
        )
        """
