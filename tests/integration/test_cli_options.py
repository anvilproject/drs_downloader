import hashlib
import os
from pathlib import Path
import tempfile

from click.testing import CliRunner

from drs_downloader.cli import cli


def test_terra():
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(cli, ['terra', '-d', dest, '--manifest_path', 'tests/fixtures/terra-data.tsv'])
        assert result.exit_code == 0

        files = sorted(next(os.walk(dest))[2])
        assert len(files) == 10
        assert files[0] == "HG00536.final.cram.crai"
        assert _verify_file(Path(dest, files[0]), 1244278, '750004f80de56fb9e74bde5a252b0260')

        assert files[9] == "NA20525.final.cram.crai"
        assert _verify_file(Path(dest, files[9]), 1337382, '91c78edd69237db468723c797d77bd30')


def test_terra_one_file():
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(cli, ['terra', '-d', dest, '--manifest_path', 'tests/fixtures/terra-one-file.tsv'])
        assert result.exit_code == 0

        files = next(os.walk(dest))[2]
        assert len(files) == 1
        assert files[0] == "HG02450.final.cram.crai"
        assert _verify_file(Path(dest, files[0]), 1405458, 'b2b6648d0b6e9f04508afda4fb815e3e')


def _verify_file(file: Path, expected_size: int, expected_md5: str) -> bool:
    """Verifies that a given file exists and has the expected size and checksum.

    Args:
        file (Path): Path to the file
        expected_size (int): Expected file size in bytes
        expected_md5 (str): Expected md5 checksum

    Returns:
        bool: True if the file passes all checks, False otherwise
    """

    if file.is_file() is False:
        return False

    actual_size = file.stat().st_size
    if actual_size != expected_size:
        return False

    md5_hash = hashlib.md5()
    md5_hash.update(open(file, 'rb').read())
    actual_md5 = md5_hash.hexdigest()

    if actual_md5 != expected_md5:
        return False

    return True


def test_terra_silent():
    """The terra command should execute without error."""
    runner = CliRunner()
    result = runner.invoke(cli, ['terra', '--silent', '--manifest_path', 'tests/fixtures/terra-data.tsv'])
    assert result.exit_code == 0


def test_gen3():
    """The gen3 command should execute without error."""
    runner = CliRunner()
    result = runner.invoke(cli, ['gen3', '--endpoint', 'https://development.aced-idp.org', '--api_key_path',
                                 'tests/fixtures/credentials.json', '--manifest_path', 'tests/fixtures/gen3-small.tsv'])
    assert result.exit_code == 0


def test_gen3_silent():
    """The gen3 command should execute without error."""
    runner = CliRunner()
    result = runner.invoke(cli, ['gen3', '--silent', '--endpoint', 'https://development.aced-idp.org', '--api_key_path',
                                 'tests/fixtures/credentials.json', '--manifest_path', 'tests/fixtures/gen3-small.tsv'])
    assert result.exit_code == 0
