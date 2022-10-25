from cli import *
# https://click.palletsprojects.com/en/8.1.x/testing/
from click.testing import CliRunner

class Cli_Test:
    def __init__(self, method, *args):
        runner = CliRunner()
        self.result = runner.invoke(method, args)
        assert self.result.exit_code == 0

def test_cli():
    test = Cli_Test(cli)

def test_config():
    test = Cli_Test(config)

def test_credentials():
    test = Cli_Test(credentials)

def test_download():
    test = Cli_Test(download)

def test_info():
    id = '028017fd-e12f-4c7d-869f-b56368194235'
    test = Cli_Test(info)

def test_list():
    test = Cli_Test(list)

def test_signed_url():
    test = Cli_Test(signed_url)


if __name__ == '__main__':
    
