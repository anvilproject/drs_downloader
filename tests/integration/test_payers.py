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
        assert any("('CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.2019-02-06_Sample_NA19712_analysis_NA19712.final\
.cram.crai', 'ERROR', 1286904, 0, ['A requestor pays AnVIL DRS URI: drs://drs.anv0:v2_c1cf1d38-f3e8-3d38-b0b8\
-cbabb7d0afa4 is specified but no Google project id is given.'])" in
                   "".join(message) for message in messages)

        assert any("('CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.2019-02-06_Sample_NA19324_analysis_NA19324.final.\
cram.crai', 'ERROR', 1286840, 0, ['A requestor pays AnVIL DRS URI: drs://drs.anv0:v2_763e1df1-9706-3fd4-90a2\
-26e914bedeca is specified but no Google project id is given.'])" in
                   "".join(message) for message in messages)

        assert any("2/4 files have downloaded successfully" in message for message in messages)
        assert result.exit_code == 1
