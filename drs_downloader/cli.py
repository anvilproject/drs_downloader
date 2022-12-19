import logging
from pathlib import Path
from typing import List
import math
import tqdm
import click
import csv

from drs_downloader.clients.gen3 import Gen3DrsClient
from drs_downloader.clients.mock import MockDrsClient
from drs_downloader.clients.terra import TerraDrsClient
from drs_downloader.manager import DrsAsyncManager

from drs_downloader import DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS

logging.basicConfig(format='%(asctime)s %(message)s',  encoding='utf-8', level=logging.INFO)
logger = logging.getLogger(__name__)  # these control our simulation


@click.group()
def cli():
    """Copy DRS objects from the cloud to your local system ."""
    pass


@cli.command()
@click.option("--silent", "-s", is_flag=True, show_default=True, default=False, help="Display nothing.")
@click.option("--destination_dir", "-d", show_default=True, default='/tmp/testing',
              help="Destination directory.")
@click.option("--manifest_path", "-m", show_default=True,
              help="Path to manifest tsv.")
@click.option('--drs_header', default='ga4gh_drs_uri', show_default=True,
              help='The column header in the TSV file associated with the DRS URIs.'
                   'Example: pfb:ga4gh_drs_uri')
def mock(silent: bool, destination_dir: str, manifest_path, drs_header):
    """Generate test files locally, without the need for server."""

    #
    # get ids from manifest
    ids_from_manifest = _extract_tsv_info(Path(manifest_path), drs_header)

    # perform downloads with a mock drs client
    _perform_downloads(destination_dir, MockDrsClient(), ids_from_manifest, silent)


@cli.command()
@click.option("--silent", "-s", is_flag=True, show_default=True, default=False, help="Display nothing.")
@click.option("--destination_dir", "-d", show_default=True,
              default="/tmp/testing", help="Destination directory.")
@click.option("--manifest_path", "-m", show_default=True,
              help="Path to manifest tsv.")
@click.option('--drs_header', default=None, help='The column header in the TSV file associated with the DRS URIs.'
              'Example: pfb:ga4gh_drs_uri')
def terra(silent: bool, destination_dir: str, manifest_path: str, drs_header: str):
    """Copy files from terra.bio"""

    # get ids from manifest
    ids_from_manifest = _extract_tsv_info(Path(manifest_path), drs_header)
    logger.info("IDS FROM MANIFEST ", ids_from_manifest)

    # perform downloads with a terra drs client
    _perform_downloads(destination_dir, TerraDrsClient(), ids_from_manifest, silent)


@cli.command()
@click.option("--silent", "-s", is_flag=True, show_default=True, default=False, help="Display nothing.")
@click.option("--destination_dir", "-d", show_default=True,
              default="/tmp/testing", help="Destination directory.")
@click.option("--manifest_path", "-m", show_default=True,
              help="Path to manifest tsv.")
@click.option('--drs_header', default='ga4gh_drs_uri', show_default=True,
              help='The column header in the TSV file associated with the DRS URIs.'
                   'Example: pfb:ga4gh_drs_uri')
@click.option('--api_key_path', show_default=True,
              help='Gen3 credentials file')
@click.option('--endpoint', show_default=True, required=True,
              help='Gen3 endpoint')
def gen3(silent: bool, destination_dir: str, manifest_path: str, drs_header: str, api_key_path: str, endpoint: str):
    """Copy files from gen3 server."""
    # read from manifest
    ids_from_manifest = _extract_tsv_info(Path(manifest_path), drs_header)

    _perform_downloads(destination_dir,
                       Gen3DrsClient(api_key_path=api_key_path, endpoint=endpoint),
                       ids_from_manifest,
                       silent)


def _perform_downloads(destination_dir, drs_client, ids_from_manifest,  silent):
    """Common helper method to run downloads."""
    # verify parameters
    if destination_dir:
        destination_dir = Path(destination_dir)
        destination_dir.mkdir(parents=True, exist_ok=True)
    # create a manager
    drs_manager = DrsAsyncManager(drs_client=drs_client, show_progress=not silent)

    # call the server, get size, checksums etc.; sort them by size
    drs_objects = drs_manager.get_objects(ids_from_manifest)
    drs_objects.sort(key=lambda x: x.size, reverse=False)
    # optimize based on workload
    drs_objects = drs_manager.optimize_workload(drs_objects)
    # determine the total number of batches
    total_batches = len(drs_objects) / DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS
    if math.ceil(total_batches) - total_batches > 0:
        total_batches += 1
        total_batches = int(total_batches)
    for chunk_of_drs_objects in tqdm.tqdm(
            DrsAsyncManager.chunker(drs_objects, DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS),
            total=total_batches,
            desc="TOTAL_DOWNLOAD_PROGRESS", leave=True):

        drs_manager.download(chunk_of_drs_objects, destination_dir)

    # show results
    if not silent:
        for drs_object in drs_objects:
            if len(drs_object.errors) > 0:
                logger.error(
                    (drs_object.name, 'ERROR', drs_object.size, len(
                        drs_object.file_parts), drs_object.errors))
            else:
                logger.info((drs_object.name, 'OK', drs_object.size, len(drs_object.file_parts)))
        logger.info(('done', 'statistics.max_files_open', drs_client.statistics.max_files_open))

    at_least_one_error = False
    for drs_object in drs_objects:
        if len(drs_object.errors) > 0:
            logger.error((drs_object.name, 'ERROR', drs_object.size, len(drs_object.file_parts), drs_object.errors))
            at_least_one_error = True
    if at_least_one_error:
        exit(1)


def _extract_tsv_info(manifest_path: Path, drs_header: str) -> List[str]:
    """Extract the DRS URI's from the provided TSV file.

    Args:
        manifest_path (str): The input file containing a list of DRS URI's.
        drs_header (str): Column header for the DRS URI's.
    Returns:
        List[str]: The URI's corresponding to the DRS objects.
    """
    assert manifest_path.is_file()

    uris = []
    uri_index = 0
    header = None
    with open(Path(manifest_path)) as file:
        tsv_file = csv.reader(file, delimiter="\t")
        headers = next(tsv_file)
        # search for header name
        if drs_header is None:
            for col in headers:
                if 'uri' in col.lower():
                    header = headers[headers.index(col)]
                    uri_index = headers.index(col)
                    break
        else:
            if drs_header in headers:
                uri_index = headers.index(drs_header)
                header = drs_header

        # add url to urls list
        if header is not None:
            for row in tsv_file:
                uris.append(row[uri_index])

        # add url to urls list
        if header is not None:
            for row in tsv_file:
                uris.append(row[uri_index])
        else:
            raise KeyError(
                f"DRS header value '{drs_header}' not found in manifest file '{manifest_path}.'"
                " Please specify a new value with the --drs_header flag.")

        for url in uris:
            if '/' in url:
                continue
            else:
                raise Exception(
                    "Check that your header name for your DRS URIS is directly above the column of your DRS URIS")

        if (len(uris) != len(set(uris))):
            raise Exception("Duplicate URIS: ", str(
                list(set([x for x in uris if uris.count(x) > 1]))), "found in your TSV file ")

    return uris


if __name__ == "__main__":
    cli()
