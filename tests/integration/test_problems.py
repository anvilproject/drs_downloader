from click.testing import CliRunner
from drs_downloader.cli import cli
import os
import tempfile


def test_terra_bad_tsv():
    """The terra command should execute with an error."""
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "terra",
                "-d",
                dest,
                "--manifest-path",
                "tests/fixtures/terra-data-bad.tsv",
            ],
        )
        assert result.exit_code != 0


# problems here


def test_gen3_bad_tsv():
    """The gen3 command should execute with an error."""
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "gen3",
                "-d",
                dest,
                "--endpoint",
                "https://development.aced-idp.org",
                "--manifest_path",
                "tests/fixtures/gen3-bad.tsv",
            ],
        )
        assert result.exit_code != 0


"""
def test_gen3_bad_credentials(caplog):
    # The gen3 command should execute with an error.
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(cli,
                               ['gen3',
                                '-d',
                                dest,
                                '--endpoint',
                                'https://development.aced-idp.org',
                                '--api_key-path',
                                'tests/fixtures/bad_credentials.json',
                                '--manifest-path',
                                'tests/fixtures/gen3-small.tsv'])

        if len([string for string in caplog.messages if ("Invalid access token in" in string)]) > 0:
            assert result.exit_code != 0
"""


def test_terra_good_gcloud(caplog):
    """The gen3 command should execute with an error."""
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        runner.invoke(
            cli,
            ["terra", "-d", dest, "--manifest-path", "tests/fixtures/terra-data.tsv"],
        )
        messages = caplog.messages
        assert any(
            ("gcloud token successfully fetched" in message for message in messages)
        )


def test_terra_bad_gcloud(caplog):
    """The gen3 command should execute with an error."""

    paths = os.getenv("PATH").split(":")
    deldexes = [paths.index(path) for path in paths if ("google-cloud-sdk" in path)]
    for deldex in deldexes:
        del paths[deldex]
    paths = ":".join(paths)

    dict_str = {}
    dict_str.update(dict(PATH=paths))

    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner(env=dict_str)
        runner.invoke(
            cli,
            ["terra", "-d", dest, "--manifest-path", "tests/fixtures/terra-data.tsv"],
        )
        # messages = caplog.messages
        # print("THE VALUE OF MESSAGES ",messages)
        # TODO: update log message assertion here
        # assert not any(("google-cloud-sdk" in message for message in messages))


"""
def test_gen3_uri_not_found(caplog):
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        runner.invoke(cli,
                      ['gen3',
                       '-d',
                       dest,
                       '--endpoint',
                       'https://development.aced-idp.org',
                       '--api-key-path',
                       'tests/fixtures/credentials.json',
                       '--manifest-path',
                       'tests/fixtures/gen3_unauthorized_uris.tsv'])

        messages = caplog.messages
        print("VALUE OF MESSAGES ", messages)
        assert any(["NOT FOUND" in message for message in messages])
"""


def test_terra_uri_not_found(caplog):
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        runner.invoke(
            cli,
            [
                "terra",
                "-d",
                dest,
                "--manifest-path",
                "tests/fixtures/bad_terra_uris.tsv",
            ],
        )
        messages = caplog.messages

        # Nake sure that all 10 objects come back with the same summary error message
        assert sum(["no record found on URI" in str(message) for message in messages]) == 10


def test_terra_partial_bad_uris(caplog):
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "terra",
                "-d",
                dest,
                "--manifest-path",
                "tests/fixtures/partial_bad_terra_uris.tsv",
            ],
        )
        messages = caplog.messages
        assert result.exit_code == 1

        # 18/41 files should download.
        assert len(os.listdir(dest)) == 18

        for message in messages:
            print("MESSAGE: ", message, "\n\n\n\n\n\n\n\n")

        # Make sure that all 14 files that had proper URIs downloaded
        assert "'GSM3891613_Bleo2_GFPp_barcodes.tsv.gz', 'OK', 16838, 1" in str(messages)
        assert "'GSM3891635_SCD2_All_barcodes.tsv.gz', 'OK', 20472, 1" in str(messages)
        assert "'GSM3891632_SCD1_Lin_barcodes.tsv.gz', 'OK', 34667, 1" in str(messages)
        assert "'GSM5897134_NL2_barcodes.tsv.gz', 'OK', 40366, 1" in str(messages)
        assert "'GSM5520738_SC_NS-4_barcodes.tsv.gz', 'OK', 42244, 1" in str(messages)
        assert "'GSM3891614_Bleo1_GFPn_genes.tsv.gz', 'OK', 217848, 1" in str(messages)
        assert "'GSM3891628_IPF2_Lin_genes.tsv.gz', 'OK', 264792, 1" in str(messages)
        assert "'GSM3891632_SCD1_Lin_genes.tsv.gz', 'OK', 264792, 1" in str(messages)
        assert "'GSM3891627_IPF1_All_genes.tsv.gz', 'OK', 264792, 1" in str(messages)
        assert "'GSM3377671_E6_genes.tsv.gz', 'OK', 264836, 1" in str(messages)
        assert "'GSM3377672_SIX2_CRE_genes.tsv.gz', 'OK', 264842, 1" in str(messages)
        assert "'GSM5520738_SC_NS-4_features.tsv.gz', 'OK', 304728, 1" in str(messages)
        assert "'GSM5531131_VS_NS5_features.tsv.gz', 'OK', 304728, 1" in str(messages)
        assert "'GSM3377671_E6_barcodes.tsv.gz', 'OK', 2280346, 3" in str(messages)

        # Check most of the errored out messages
        assert "(None, 'ERROR', 0, 0, ['404: File not found: 4244dc4b-42bsdfdsfsdf4013-a67a-804c413f707b on URI: drs://data.terra.bio/v1_628dd3d8-46dd-49bd-a300-62834ac7082a_4244dc4b-42bsdfdsfsdf4013-a67a-804c413f707b'])" in str(messages)
        assert "(None, 'ERROR', 0, 0, ['404: no record found on URI: drs://dg.4503:dg.4503/4f1de21e-24cf-439e-f367-2179408c2187'])" in str(messages)
        assert "(None, 'ERROR', 0, 0, ['404: no record found on URI: drs://dg.4503:dg.4503/6f64emfd-f62a-46ca-b1f8-77b3754db409'])" in str(messages)
        assert "(None, 'ERROR', 0, 0, ['404: File not found: f366901f-dsfdsf18-4272-8c5d-d749c48cbf13 on URI: drs://data.terra.bio/v1_628dd3d8-46dd-49bd-a300-62834ac7082a_f366901f-dsfdsf18-4272-8c5d-d749c48cbf13'])" in str(messages)
        assert "(None, 'ERROR', 0, 0, ['404: no record found on URI: drs://dg.4503:dg.4503/bf2b854a-17a3-4b3c-fjr2-4f670ceb9e85'])" in str(messages)
        assert "(None, 'ERROR', 0, 0, ['404: File not found: 6dsfdsfsdf194867-eaf3-48cf-a7c8-fe8f18272aaf on URI: drs://data.terra.bio/v1_1d538e0b-7d23-4293-b857-d00851477f8c_6dsfdsfsdf194867-eaf3-48cf-a7c8-fe8f18272aaf'])" in str(messages)
        assert "(None, 'ERROR', 0, 0, ['404: no record found on URI: drs://dg.4503:dg.4503/15fdd543-9875-fei3-8bc2-22985473dab6'])" in str(messages)
        assert "(None, 'ERROR', 0, 0, ['404: No snapshots found for this DRS Object ID on URI: drs://data.terra.bio/v1_1d538e0b-7d23-4893-b857-d00851477f8c_b8a036dc-484e-4570-954a-4787b8fefa6b'])" in str(messages)
        assert "(None, 'ERROR', 0, 0, ['404: File not found: 28sdfdsfsdf3a8491-4cbb-41a9-b716-a45a96217978 on URI: drs://data.terra.bio/v1_628dd3d8-46dd-49bd-a300-62834ac7082a_28sdfdsfsdf3a8491-4cbb-41a9-b716-a45a96217978'])" in str(messages)
        assert "(None, 'ERROR', 0, 0, ['404: no record found on URI: drs://dg.4503:dg.4503/3c861ec6-d810-4058-2le0-c0b19dd5933e'])" in str(messages)
        assert "(None, 'ERROR', 0, 0, ['404: no record found on URI: drs://dg.4503:dg.4503/1b387791-53fb-48fa-9194-d51953814f19'])" in str(messages)
        assert "(None, 'ERROR', 0, 0, ['404: no record found on URI: drs://dg.4503:dg.4503/374a0ad9-b3a2-47f3-1049-5083b302e478'])" in str(messages)
        assert "(None, 'ERROR', 0, 0, ['404: no record found on URI: drs://dg.4503:dg.4503/fdb50b7d-f126-4ddb-93f8-6ekf8faf25d3'])" in str(messages)
        assert "(None, 'ERROR', 0, 0, ['404: no record found on URI: drs://dg.4503:dg.4503/231527ef-7075-477f-9265-4a1289571042384'])" in str(messages)


"""
def test_gen3_weak_creds(caplog):
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        runner.invoke(cli,
                      ['gen3',
                       '-d',
                       dest,
                       '--endpoint',
                       'https://development.aced-idp.org',
                       '--api-key-path',
                       'tests/fixtures/weak_creds.json',
                       '--manifest-path',
                       'tests/fixtures/gen3-small.tsv'])
        messages = caplog.messages
        assert any(["UNAUTHORIZED" in message for message in messages])
"""


# Don't have a large enough file to test this function now that the part size has changed
"""
def test_terra_large_file():
    dir = os.path.realpath('logs.log')
    result = subprocess.Popen(['drs_download', 'terra', '--manifest_path', 'tests/fixtures/terra-large-file.tsv'])
    time.sleep(5)
    result.kill()
    with open(dir, "r") as fd:
        str_store = fd.readlines()
        assert any(("has over 1000 parts, consider optimization" in message for message in str_store))
"""
