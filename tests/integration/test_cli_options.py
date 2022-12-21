import os
import subprocess

from click.testing import CliRunner
import time
from drs_downloader.cli import cli


def test_terra(tmp_path, caplog):

    runner = CliRunner()
    result = runner.invoke(cli, ['terra', '-d', tmp_path, '--manifest-path', 'tests/fixtures/terra-data.tsv'])
    assert result.exit_code == 0

    files = sorted(os.listdir(tmp_path))
    assert len(files) == 10
    assert files[0] == "HG00536.final.cram.crai"
    assert files[9] == "NA20525.final.cram.crai"
    assert f"Downloading to: {tmp_path}" in caplog.messages

    result = runner.invoke(cli, ['terra', '--manifest-path', 'tests/fixtures/terra-data.tsv'])
    assert result.exit_code == 0

    files = [file for file in sorted(os.listdir(os.getcwd())) if "final.cram.crai" in file]
    assert len(files) == 10
    assert files[0] == "HG00536.final.cram.crai"
    assert files[9] == "NA20525.final.cram.crai"
    assert f"Downloading to: {os.getcwd()}" in caplog.messages


def test_terra_default_cwd():
    runner = CliRunner()
    pre_file_count = len(sorted(next(os.walk(os.getcwd()))[2]))
    result = runner.invoke(cli, ['terra', '--manifest-path', 'tests/fixtures/terra-data.tsv'])
    post_file_count = len(sorted(next(os.walk(os.getcwd()))[2]))

    assert result.exit_code == 0
    assert (post_file_count - pre_file_count) == 10


def test_terra_different_header(tmp_path, caplog):
    runner = CliRunner()
    result = runner.invoke(cli, ['terra', '-d', tmp_path, '--manifest-path',
                                 'tests/fixtures/terra-different-header.tsv', '--drs-column-name', 'drs_uri'])
    assert result.exit_code == 0

    files = sorted(os.listdir(tmp_path))
    assert len(files) == 10
    assert files[0] == "HG00536.final.cram.crai"
    assert files[9] == "NA20525.final.cram.crai"

    result = runner.invoke(cli, ['terra', '-d', tmp_path, '--manifest-path',
                                 'tests/fixtures/terra-different-header.tsv', '--drs-column-name', 'foo'])
    assert result.exit_code == 1
    assert (
        "DRS header value 'foo' not found in manifest file tests/fixtures/terra-different-header.tsv."
        in result.exception.args[0]
    )


def test_terra_silent():
    """The terra command should execute without error."""
    runner = CliRunner()
    result = runner.invoke(cli, ['terra', '--silent', '--manifest-path', 'tests/fixtures/terra-data.tsv'])
    assert result.exit_code == 0


def test_optimizer_part_size_large_file():
    dir = os.path.realpath('drs_downloader.log')
    result = subprocess.Popen(['drs_download', 'terra', '--manifest-path', 'tests/fixtures/mixed_file_sizes.tsv'])
    time.sleep(5)
    result.kill()
    with open(dir, "r") as fd:
        str_store = fd.readlines()
        assert any(("part_size=134217728" in message for message in str_store))


def test_gen3():
    """The gen3 command should execute without error."""
    runner = CliRunner()
    result = runner.invoke(cli, ['gen3', '--endpoint', 'https://development.aced-idp.org', '--api-key-path',
                                 'tests/fixtures/credentials.json', '--manifest-path', 'tests/fixtures/gen3-small.tsv'])
    assert result.exit_code == 0


def test_gen3_silent():
    """The gen3 command should execute without error."""
    runner = CliRunner()
    result = runner.invoke(cli, ['gen3', '--silent', '--endpoint', 'https://development.aced-idp.org', '--api-key-path',
                                 'tests/fixtures/credentials.json', '--manifest-path', 'tests/fixtures/gen3-small.tsv'])
    assert result.exit_code == 0
