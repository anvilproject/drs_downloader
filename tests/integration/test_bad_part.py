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
        for file in os.listdir(Path('tests/fixtures/interrupted_download')):
            shutil.copy2(Path(f'tests/fixtures/interrupted_download/{file}'), dest)

        bad_part = Path(dest, 'HG00622.final.cram.crai.0.1048576.part')

        # Overwrite the first few bytes of the part file with a string representing malformed or new data.
        # This will allow the bad part file to pass the filesize check but not the checksum validation.
        with open(bad_part, 'r+b') as f:
            f.seek(0)
            f.write(b'bad data')

        runner = CliRunner()
        result = runner.invoke(
            cli, ['terra', '-d', dest, '--manifest-path',  'tests/fixtures/terra-data.tsv']
        )
        assert result.exit_code == 1

        messages = caplog.messages
        # Assert that the part file passed the filesize check
        assert (
            'HG00622.final.cram.crai.0.1048576.part exists and has expected size. Skipping download.'
            in messages
        )

        #Assert that the file with the bad part was caught in the checksum step
        assert (
            'Actual md5 hash 9b507ccc2a8abb463e6ba128d9c957c5 does not match expected 238eb9fce97703ae1b9b6b6aaa00b0f3'
            in messages
        )
