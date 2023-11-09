from click.testing import CliRunner
from drs_downloader.cli import cli
import tempfile


def test_invalid_destination_path(caplog):
    """The terra command should execute with an error."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "terra",
            "-d",
            "/\i/\/\/\/\\jjon]k\DATA",
            "-m",
            "tests/fixtures/Non_AnVIL.tsv"
        ],
    )
    messages = caplog.messages
    assert messages[0].startswith("Invalid --destination-dir path provided:")
    assert result.exit_code == 1


def test_terra_uri_not_found(caplog):
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        runner.invoke(
            cli,
            [
                "terra",
                "-d",
                dest,
                "-m",
                "tests/fixtures/terra_uppercase_uri_prefix.tsv"
            ],
        )
        messages = caplog.messages
        print(messages)
        assert "ERROR: AnVIL Drs URIS starting with  'drs://drs.anv0:' or 'drs://dg.anv0: were \
provided in the manifest but no Terra workspace Google project id was given. Specify one with \
the --user-project option" in str(messages)
        # Nake sure that all 10 objects come back with the same summary error message
        # assert sum(["no record found on URI" in str(message) for message in messages]) == 10
