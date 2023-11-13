from click.testing import CliRunner
from drs_downloader.cli import cli
import tempfile


def test_terra_bad_project_id(caplog):
    """The terra command should execute with an error."""
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "terra",
                "-d",
                dest,
                "-m",
                "tests/fixtures/terra-data.tsv",
                "--user-project",
                "terra-abcdefgh"
            ],
        )
        messages = caplog.messages
        # 9 AnVIL files to be downloaded so there should be 9 errors returning on the invalid project ID
        assert sum("User project specified in --user-project option is invalid" in message for message in messages) == 9
        assert result.exit_code != 0


def test_terra_bad_project_id_mixed_data(caplog):
    """The terra command should execute with an error."""
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "terra",
                "-d",
                dest,
                "-m",
                "tests/fixtures/mixed_uris.tsv",
                "--user-project",
                "terra-abcdefgh"
            ],
        )
        messages = caplog.messages
        assert any("('GSM3891613_Bleo2_GFPp_barcodes.tsv.gz', 'OK', 16838, 1)" in
                   "".join(message) for message in messages)
        assert any("('GSM5520738_SC_NS-4_features.tsv.gz', 'OK', 304728, 1)" in
                   "".join(message) for message in messages)
        assert sum("User project specified in --user-project option is invalid" in message for message in messages) == 2
        assert any("2/4 files have downloaded successfully" in message for message in messages)
        assert result.exit_code == 1


def test_no_project_id_specified_mixed_data(caplog):
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "terra",
                "-d",
                dest,
                "-m",
                "tests/fixtures/mixed_uris.tsv",
            ],
        )
        messages = caplog.messages
        assert result.exit_code == 1
        assert messages[0] == "ERROR: AnVIL Drs URIS starting with  'drs://drs.anv0:' or 'drs://dg.anv0: were\
provided in the manifest but no Terra workspace Google project id was given. Specify one with\
the --user-project option"
