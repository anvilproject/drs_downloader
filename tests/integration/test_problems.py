from click.testing import CliRunner
from drs_downloader.cli import cli


def test_terra_bad_tsv():
    """The terra command should execute with an error."""
    runner = CliRunner()
    result = runner.invoke(cli, ['terra', '--manifest-path', 'tests/fixtures/terra-data-bad.tsv'])
    assert result.exit_code == 1

    result = runner.invoke(cli, ['terra', '--manifest_path', 'tests/fixtures/terra-data.tsv',
                                 '--destination_dir', 'DATA'])
    assert result.exit_code == 2

    result = runner.invoke(cli, ['terra', '--manifest_path', 'tests/fixtures/terra-data.tsv',
                                 '--drs_header', 'pfb:ga4gh_drs_uri'])
    assert result.exit_code == 2


def test_gen3_bad_tsv():
    """The gen3 command should execute with an error."""
    runner = CliRunner()
    result = runner.invoke(cli, ['gen3', '--endpoint', 'https://development.aced-idp.org', '--manifest-path',
                                 'tests/fixtures/gen3-bad.tsv'])
    assert result.exit_code == 1
