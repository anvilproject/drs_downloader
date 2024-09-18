import os
import shutil
import subprocess
import tempfile
import hashlib
from pathlib import Path
import base64


from click.testing import CliRunner
import time
from drs_downloader.cli import cli


def test_duplicate_uris():
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "terra",
                "-v",
                "-d",
                dest,
                "--manifest-path",
                "tests/fixtures/terra-data-duplicates.tsv",
            ],
        )
        assert result.exit_code == 1
        assert result.exception


def test_terra_one_file():
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "terra",
                "-v",
                "-d",
                dest,
                "--manifest-path",
                "tests/fixtures/terra-one-file.tsv",
            ],
        )
        assert result.exit_code == 0

        files = next(os.walk(dest))[2]
        assert len(files) == 1
        print("FIELS: ", files[0])
        assert files[0] == "CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.gVCF.\
2019-02-06_Sample_NA11830_analysis_NA11830.haplotypeCalls.er.raw.g.vcf.gz.tbi"
        assert _verify_file(
            Path(dest, files[0]), 4080487, "n1k2cGgCESLAAn1qlRUAwA=="
        )


def _verify_file(file: Path, expected_size: int, expected_md5: str) -> bool:
    """Verifies that a given file exists and has the expected size and checksum.

    Args:
        file (Path): Path to the file
        expected_size (int): Expected file size in bytes
        expected_md5 (str): Expected md5 checksum

    Returns:
        bool: True if the file passes all checks, False otherwise
    """

    if file.is_file() is False:
        return False

    actual_size = file.stat().st_size
    if actual_size != expected_size:
        return False

    # hashes in new test file are Base64(unhex(MD5($plaintext)))
    # so you have to md5 hash, hex, and then b64 encode to compare to the testing file hashes
    hash = bytes.fromhex(hashlib.md5(open(file, "rb").read()).hexdigest())
    base64_encoded = base64.b64encode(hash).decode()
    actual_md5 = base64_encoded

    if actual_md5 != expected_md5:
        return False

    return True


def test_terra(tmp_path, caplog):

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["terra", "-d", tmp_path, "--verbose", "--manifest-path", "tests/fixtures/terra-data.tsv"],
    )
    assert result.exit_code == 0

    files = sorted(os.listdir(tmp_path))
    assert len(files) == 9
    assert files[0] == "CCDG_13607_Project_CCDG_13607_B01_GRM_WGS.cram.\
2019-02-06_Sample_HG00188_analysis_HG00188.final.cram.crai"
    assert files[8] == "CCDG_14151_Project_CCDG_14151_B01_GRM_WGS.cram.\
2020-02-12_Sample_NA12344_analysis_NA12344.final.cram.crai"
    assert f"Downloading to: {tmp_path}" in caplog.messages

    result = runner.invoke(
        cli, ["terra", "--manifest-path", "tests/fixtures/terra-data.tsv"]
    )
    assert result.exit_code == 0

    files = [
        file for file in sorted(os.listdir(os.getcwd())) if "final.cram.crai" in file
    ]
    # assert len(files) == 10
    # assert files[0] == "HG00536.final.cram.crai"
    # assert files[9] == "NA20525.final.cram.crai"
    # assert f"Downloading to: {os.getcwd()}" in caplog.messages


def test_terra_default_cwd():
    runner = CliRunner()
    pre_file_count = len(sorted(next(os.walk(os.getcwd()))[2]))
    result = runner.invoke(
        cli,
        ["terra", "-v", "--duplicate", "--manifest-path", "tests/fixtures/terra-data.tsv"],
    )
    post_file_count = len(sorted(next(os.walk(os.getcwd()))[2]))

    assert result.exit_code == 0
    assert (post_file_count - pre_file_count) == 9


"""
def test_terra_different_header(tmp_path, caplog):
    runner = CliRunner()
    result = runner.invoke(cli, ['terra', '-d', tmp_path, '--manifest-path',
                                 'tests/fixtures/terra-different-header.tsv', '--drs-column-name', 'drs_uri'])
    assert result.exit_code == 0

    files = sorted(os.listdir(tmp_path))
    assert len(files) == 10
    assert files[0] == "HG00536.final.cram.crai"
    assert files[9] == "NA20525.final.cram.crai"

    result = runner.invoke(cli, ['terra', '-d', tmp_path, '--manifest-path',
                                 'tests/fixtures/terra-different-header.tsv', '--drs-column-name', 'foo'])
    assert result.exit_code == 1
    assert (
        "DRS header value 'foo' not found in manifest file tests/fixtures/terra-different-header.tsv."
        in result.exception.args[0]
    )
"""


def test_terra_silent():
    """The terra command should execute without error."""
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "terra",
                "-v",
                "-d",
                dest,
                "--verbose",
                "--manifest-path",
                "tests/fixtures/terra-data.tsv",
            ],
        )
        assert result.exit_code == 0


def test_optimizer_part_size(caplog):
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        runner.invoke(
            cli,
            ["terra", "-v", "-d", dest, "--manifest-path", "tests/fixtures/terra-data.tsv"],
        )
        print(f"THE VLAUE OF MESSAGES {str(caplog.messages)}")
        part_size = int(
            [
                caplog.messages[caplog.messages.index(message)]
                for message in caplog.messages
                if ("part_size" in message)
            ][0].split("=")[-1]

        )
        print(f'============== THE VALUE OF PART SIZE {int(part_size)}')

        assert part_size == (1 * 1024**2)


def test_optimizer_simul_part_handlers(caplog):
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        runner.invoke(
            cli,
            ["terra", "-d", dest, "-v", "--manifest-path", "tests/fixtures/terra-data.tsv"],
        )
        messages = caplog.messages
        # print("THE VALUE OF MESSAGES ", messages)
        part_handlers = int(
            [
                messages[messages.index(message)]
                for message in messages
                if ("part_handlers" in message)
            ][0].split("=")[-1]
        )
        assert part_handlers == 2


def test_optimizer_part_size_large_file():
    dir = os.path.realpath("drs_downloader.log")
    result = subprocess.Popen(
        [
            "drs_download",
            "terra",
            "-v",
            "--manifest-path",
            "tests/fixtures/mixed_file_sizes.tsv",
        ]
    )
    time.sleep(8)
    result.kill()
    with open(dir, "r") as fd:
        str_store = fd.readlines()
        assert any(("part_size=134217728" in message for message in str_store))


"""
def test_gen3():
        The gen3 command should execute without error.
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(cli,
                               ['gen3',
                                '-d',
                                dest,
                                '--endpoint',
                                'https://development.aced-idp.org',
                                '--api-key-path',
                                'tests/fixtures/credentials.json',
                                '--manifest-path',
                                'tests/fixtures/gen3-small.tsv'])
        assert result.exit_code == 0



def test_gen3_silent():
    # The gen3 command should execute without error.
    with tempfile.TemporaryDirectory() as dest:
        runner = CliRunner()
        result = runner.invoke(cli,
                               ['gen3',
                                '-d',
                                dest,
                                '--silent',
                                '--endpoint',
                                'https://development.aced-idp.org',
                                '--api-key-path',
                                'tests/fixtures/credentials.json',
                                '--manifest-path',
                                'tests/fixtures/gen3-small.tsv'])
        assert result.exit_code == 0

# this function errors
"""


def test_terra_rename():
    with tempfile.TemporaryDirectory() as dest:
        for file in os.listdir(Path("tests/fixtures/rename")):
            shutil.copy2(Path("tests/fixtures/rename", file), dest)

        _, _, files = next(os.walk(dest))
        file_count = len(files)

        runner = CliRunner()
        runner.invoke(
            cli,
            [
                "terra",
                "-d",
                dest,
                "--manifest-path",
                "tests/fixtures/terra-data.tsv",
                "--duplicate",
            ],
        )
        _, _, files_after = next(os.walk(dest))
        file_count_after = len(files_after)

        assert (file_count_after - file_count) == 9


def test_terra_do_not_rename():
    with tempfile.TemporaryDirectory() as dest:
        for file in os.listdir(Path("tests/fixtures/rename")):
            shutil.copy2(Path("tests/fixtures/rename", file), dest)

        _, _, files = next(os.walk(dest))
        file_count = len(files)

        runner = CliRunner()
        runner.invoke(
            cli,
            ["terra", "-d", dest, "--manifest-path", "tests/fixtures/terra-data.tsv"],
        )
        _, _, files_after = next(os.walk(dest))
        file_count_after = len(files_after)

        assert (file_count_after - file_count) == 9
