from click.testing import CliRunner
from drs_downloader.cli import cli


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
