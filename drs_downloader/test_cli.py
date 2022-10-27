from cli import *
from click.testing import CliRunner
import cProfile
import cli

class Cli_Test:
    """
    A class for streamlining the test of the Click command line interface.
    Reference:
        https://click.palletsprojects.com/en/8.1.x/testing/
    Example:
        test = Cli_Test(cli, ['version'])
    """
    def __init__(self, method, *args):
        runner = CliRunner()
        self.result = runner.invoke(method, args)
        assert self.result.exit_code == 0

def test_cli():
    assert True

def test_config():
    assert True

def test_credentials():
    assert True

def test_download():
    assert True

def test_info():
    id = '028017fd-e12f-4c7d-869f-b56368194235'
    assert True

def test_list():
    assert True

def test_get_signed_url():
    credentials = 'Secrets/credentials.json'
    uris = []
    with open('uris.txt') as uris_file:
        uris = uris_file.read().splitlines()
    cli.get_signed_url(credentials, uris)
    assert True


if __name__ == '__main__':
    test_download()
