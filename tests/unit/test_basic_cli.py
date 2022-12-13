import os.path
from click.testing import CliRunner
from drs_downloader.cli import cli


def test_license():
    """The project should have license."""
    assert os.path.isfile('./LICENSE')


def test_mock():
    """The mock command should execute without error."""
    runner = CliRunner()
    result = runner.invoke(cli, ['mock'])
    assert result.exit_code == 0