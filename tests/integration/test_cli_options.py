import os
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
        assert files[9] == "NA20525.final.cram.crai"


def test_terra_silent():
    """The terra command should execute without error."""
    runner = CliRunner()
    result = runner.invoke(cli, ['terra', '--silent', '--manifest_path', 'tests/fixtures/terra-data.tsv'])
    assert result.exit_code == 0


def test_optimizer_part_size(caplog):
    runner = CliRunner()
    runner.invoke(cli, ['terra', '--manifest_path', 'tests/fixtures/terra-data.tsv'])
    messages =caplog.messages
    print("**************************************************THE VALUE OF MESSAGES ",messages)
    part_size = int([messages[messages.index(message)] for message in messages if('part_size' in message)][0].split("=")[-1])
    assert part_size == (5 * 1024 ** 2)

def test_optimizer_simul_part_handlers(caplog):
    runner = CliRunner()
    runner.invoke(cli, ['terra', '--manifest_path', 'tests/fixtures/terra-data.tsv'])
    messages =caplog.messages
    print("THE VALUE OF MESSAGES ",messages)
    part_handlers = int([messages[messages.index(message)] for message in messages if('part_handlers' in message)][0].split("=")[-1])
    assert part_handlers == 1

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
