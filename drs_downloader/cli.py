import click

@click.group(no_args_is_help=True)
def cli():
    """Welcome to the drs_downloader."""
    print(cli.__doc__)
    
@cli.command()
@click.option('--url', default=None, show_default=True, help='Signed URL')
def download():
    """Downloads the DRS object."""
    return True

@cli.command()
def signed_url():
    """Downloads a DRS object from a signed URL."""
    return True

@cli.command()
def list():
    """Lists all DRS Objects at a given endpoint."""
    return True

@cli.command()
@click.option('--uri', default=None, show_default=True, help='URI of the file of interest')
def info():
    """Displays information of a given DRS object."""
    return True

@cli.command()
def config():
    """Configures the downloader."""
    return True

@cli.command()
def credentials():
    """Authenticates the user."""
    return True
    
if __name__ == '__main__':
    cli()
