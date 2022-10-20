import click


@click.command()
def cli():
    """Welcome to drs_downloader."""
    print(cli.__doc__)


if __name__ == '__main__':
    cli()
