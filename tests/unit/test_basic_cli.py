import json
import os.path

from click.testing import CliRunner

from drs_downloader.cli import cli
import tempfile

from drs_downloader.clients.mock import manifest_all_ok, manifest_bad_file_size, manifest_bad_id_for_download


def test_license():
    """The project should have license."""
    assert os.path.isfile('./LICENSE')


def test_mock_all_ok(number_of_object_ids=10):
    """The mock command should execute without error."""
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()

        # create a test manifest
        tsv_file = manifest_all_ok(number_of_object_ids)
        print(tsv_file.name)

        result = runner.invoke(cli, ['mock', '-d', dest, '--manifest-path', tsv_file.name])

        assert result.exit_code == 0

        # leave test manifest in place if an error
        os.unlink(tsv_file.name)


def test_mock_bad_file_size(caplog):
    """The mock command should return an error.

    Args:
        caplog (object): https://docs.pytest.org/en/7.1.x/how-to/logging.html#caplog-fixture
    """
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()

        # create a test manifest
        tsv_file = manifest_bad_file_size()

        print(tsv_file.name)
        result = runner.invoke(cli, ['mock', '-d', dest, '--manifest-path', tsv_file.name])

        # should return non zero
        assert result.exit_code != 0
        # should log an exception
        # # assert (
        #     len([r for r in caplog.records if 'does not match expected size' in json.dumps(r.msg)]) == 1,
        #     caplog.records
        # )

        # leave test manifest in place if an error
        os.unlink(tsv_file.name)


def test_mock_bad_id(caplog):
    """The mock command should return an error.

    Args:
        caplog (object): https://docs.pytest.org/en/7.1.x/how-to/logging.html#caplog-fixture
    """
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()

        # create a test manifest
        tsv_file = manifest_bad_id_for_download()
        runner.invoke(cli, ['mock', '-d', dest, '--manifest-path', tsv_file.name])
        assert len([msg for msg in caplog.messages if 'ERROR' in msg]) > 0, caplog.records
        # leave test manifest in place if an error
        os.unlink(tsv_file.name)
