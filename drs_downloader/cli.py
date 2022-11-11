import logging
import uuid
from pathlib import Path
from typing import List

import click
import pandas as pd

from drs_downloader.clients.mock import MockDrsClient
from drs_downloader.clients.terra import TerraDrsClient
from drs_downloader.manager import DrsAsyncManager

logging.basicConfig(format='%(asctime)s %(message)s',  encoding='utf-8', level=logging.INFO)
logger = logging.getLogger(__name__)

# these control our simulation
NUMBER_OF_OBJECT_IDS = 10


@click.group()
def cli():
    """Copy DRS objects from the cloud to your local system ."""
    pass


@cli.command()
@click.option("--silent", "-s", is_flag=True, show_default=True, default=False, help="Display nothing.")
@click.option("--destination_dir", "-d", show_default=True, default='/tmp/testing', help="Destination directory.")
def mock(silent: bool, destination_dir: str):
    """Generate test files locally, without the need for server."""
    if destination_dir:
        destination_dir = Path(destination_dir)

    # get a drs client
    drs_client = MockDrsClient()

    # assign it to a manager
    drs_manager = DrsAsyncManager(drs_client=drs_client, show_progress=not silent)

    # simulate read from manifest
    ids_from_manifest = [str(uuid.uuid4()) for _ in range(NUMBER_OF_OBJECT_IDS)]

    # get the DrsObject for the ids
    drs_objects = drs_manager.get_objects(ids_from_manifest)

    # shape the workload
    drs_objects = drs_manager.optimize_workload(drs_objects)

    # download the objects, now with file_parts
    drs_objects = drs_manager.download(drs_objects, destination_dir)

    # show results
    if not silent:
        for drs_object in drs_objects:
            if len(drs_object.errors) > 0:
                logger.error((drs_object.name, 'ERROR', drs_object.size, len(drs_object.file_parts), drs_object.errors))
            else:
                logger.info((drs_object.name, 'OK', drs_object.size, len(drs_object.file_parts)))
        logger.info(('done', 'statistics.max_files_open', drs_client.statistics.max_files_open))


@cli.command()
@click.option("--silent", "-s", is_flag=True, show_default=True, default=False, help="Display nothing.")
@click.option("--destination_dir", "-d", show_default=True, default='/tmp/testing', help="Destination directory.")
@click.option("--manifest_path", "-m", show_default=True, default='tests/fixtures/terra-data.tsv',
              help="Path to manifest tsv.")
def terra(silent: bool, destination_dir: str, manifest_path: str):
    """Copy files from terra.bio"""

    if destination_dir:
        destination_dir = Path(destination_dir)
    if manifest_path:
        manifest_path = Path(manifest_path)

    # get a drs client
    drs_client = TerraDrsClient()

    # assign it to a manager
    drs_manager = DrsAsyncManager(drs_client=drs_client, show_progress=not silent)

    # read from manifest
    def _extract_tsv_info(manifest_path_: Path, drs_header: str = 'pfb:ga4gh_drs_uri') -> List[str]:
        """Extract the DRS URI's from the provided TSV file.

        Args:
            manifest_path_ (str): The input file containing a list of DRS URI's.
            drs_header (str): Column header for the DRS URI's.
        Returns:
            List[str]: The URI's corresponding to the DRS objects.
        """
        uris = []

        df = pd.read_csv(manifest_path_, sep='\t')
        if drs_header in df.columns.values.tolist():
            for i in range(df[drs_header].count()):
                uris.append(df['pfb:ga4gh_drs_uri'][i])
        else:
            raise KeyError(f"Header '{drs_header}' not found in {manifest_path}")

        return uris

    ids_from_manifest = _extract_tsv_info(manifest_path)

    # get the DrsObject for the ids
    drs_objects = drs_manager.get_objects(ids_from_manifest)

    # shape the workload
    drs_objects = drs_manager.optimize_workload(drs_objects)

    # download the objects, now with file_parts
    drs_objects = drs_manager.download(drs_objects, destination_dir)

    # show results
    if not silent:
        for drs_object in drs_objects:
            if len(drs_object.errors) > 0:
                logger.error((drs_object.name, 'ERROR', drs_object.size, len(drs_object.file_parts), drs_object.errors))
            else:
                logger.info((drs_object.name, 'OK', drs_object.size, len(drs_object.file_parts)))
        logger.info(('done', 'statistics.max_files_open', drs_client.statistics.max_files_open))


if __name__ == "__main__":
    cli()
