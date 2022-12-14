from click.testing import CliRunner
from drs_downloader.cli import cli


def test_terra_bad():
    """The terra command should execute without error."""
    runner = CliRunner()
    result = runner.invoke(cli, ['terra', '--manifest_path', 'tests/fixtures/terra-data-bad.tsv'])
    assert result.exit_code != 0


def test_gen3_problem():
    """The gen3 command should execute without error."""
    runner = CliRunner()
    result = runner.invoke(cli, ['gen3', '--endpoint', 'https://development.aced-idp.org', '--manifest_path',
                                 'tests/fixtures/gen3-bad.tsv'])
    assert result.exit_code != 0
