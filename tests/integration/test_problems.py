from click.testing import CliRunner
from drs_downloader.cli import cli
import os
import tempfile


def test_terra_bad_tsv():
    """The terra command should execute with an error."""
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(cli, ['terra', '-d', dest, '--manifest-path', 'tests/fixtures/terra-data-bad.tsv'])
        assert result.exit_code != 0

# problems here


def test_gen3_bad_tsv():
    """The gen3 command should execute with an error."""
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(cli,
                               ['gen3',
                                '-d',
                                dest,
                                '--endpoint',
                                'https://development.aced-idp.org',
                                '--manifest_path',
                                'tests/fixtures/gen3-bad.tsv'])
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
        runner.invoke(cli, ['terra', '-d', dest, '--manifest-path', 'tests/fixtures/terra-data.tsv'])
        messages = caplog.messages
        assert any(("google-cloud-sdk" in message for message in messages))


def test_terra_bad_gcloud(caplog):
    """The gen3 command should execute with an error."""

    paths = os.getenv('PATH').split(":")
    deldexes = [paths.index(path) for path in paths if ("google-cloud-sdk" in path)]
    for deldex in deldexes:
        del (paths[deldex])
    paths = ':'.join(paths)

    dict_str = {}
    dict_str.update(dict(PATH=paths))

    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner(env=dict_str)
        runner.invoke(cli, ['terra', '-d', dest, '--manifest-path', 'tests/fixtures/terra-data.tsv'])
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
        runner.invoke(cli, ['terra', '-d', dest, '--manifest-path', 'tests/fixtures/bad_terra_uris.tsv'])
        messages = caplog.messages
        assert any(["Not Found" in message for message in messages])


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
