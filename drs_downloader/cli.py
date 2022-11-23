import logging
import uuid
from pathlib import Path
from typing import List
import math
import tqdm
import click
import csv

from drs_downloader.clients.mock import MockDrsClient
from drs_downloader.clients.terra import TerraDrsClient
from drs_downloader.manager import DrsAsyncManager

from drs_downloader import DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS

# logging.basicConfig(format='%(asctime)s %(message)s',  encoding='utf-8', level=logging.INFO)
logger = logging.getLogger(__name__)  # these control our simulation
NUMBER_OF_OBJECT_IDS = 10


@click.group()
def cli():
    """Copy DRS objects from the cloud to your local system ."""
    pass


@cli.command()
@click.option("--silent", "-s", is_flag=True, show_default=True, default=False, help="Display nothing.")
@click.option("--destination_dir", "-d", show_default=True, default='/tmp/testing',
              help="Destination directory.")
def mock(silent: bool, destination_dir: str):
    """Generate test files locally, without the need for server."""
    if destination_dir:
        destination_dir = Path(destination_dir)
        destination_dir.mkdir(parents=True, exist_ok=True)

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

    for drs_object in drs_objects:
        at_least_one_error = False
        if len(drs_object.errors) > 0:
            logger.error((drs_object.name, 'ERROR', drs_object.size, len(drs_object.file_parts), drs_object.errors))
            at_least_one_error = True
    if at_least_one_error:
        exit(99)


@cli.command()
@click.option("--silent", "-s", is_flag=True, show_default=True, default=False, help="Display nothing.")
@click.option("--destination_dir", "-d", show_default=True,
              default="/tmp/testing", help="Destination directory.")
@click.option("--manifest_path", "-m", show_default=True, default='tests/fixtures/terra-data.tsv',
              help="Path to manifest tsv.")
@click.option('--drs_header', default=None, help='The column header in the TSV file associated with the DRS URIs.'
              'Example: pfb:ga4gh_drs_uri')
def terra(silent: bool, destination_dir: str, manifest_path: str, drs_header: str):
    """Copy files from terra.bio"""

    if destination_dir:
        destination_dir = Path(destination_dir)
        destination_dir.mkdir(parents=True, exist_ok=True)
    if manifest_path:
        manifest_path = Path(manifest_path)

    # get a drs client
    drs_client = TerraDrsClient()

    # assign it to a manager
    drs_manager = DrsAsyncManager(drs_client=drs_client, show_progress=not silent)

    # read from manifest
    # print("THE VALUE OF MANIFEST PATH", manifest_path)
    ids_from_manifest = _extract_tsv_info(manifest_path, drs_header)

    drs_objects = drs_manager.get_objects(ids_from_manifest)
    drs_objects.sort(key=lambda x: x.size, reverse=False)

    # print("THE VALUE OF DRS OBJECTS ", drs_objects[0])
    total_batches = len(ids_from_manifest) / DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS

    if (math.ceil(total_batches) - total_batches > 0):
        total_batches += 1
        total_batches = int(total_batches)

    ids_list = [objects.id for objects in drs_objects]

    # for object.id in drs_objects
    start = 0
    for chunk_of_object_ids in tqdm.tqdm(
            DrsAsyncManager._chunker(ids_list, DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS),
            total=total_batches,
            desc="TOTAL_DOWNLOAD_PROGRESS", leave=True):

        # for some reason the objectgs don't stay sorted so you need to sort them
        # here to make sure that the right files are matching up
        drs_objects_sign = drs_manager.get_signed_urls(chunk_of_object_ids)
        drs_objects_sign.sort(key=lambda x: x.size, reverse=False)
        drs_objects.sort(key=lambda x: x.size, reverse=False)

        i = 0
        for start in range(start, start + DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS):
            if (start > len(drs_objects) - 1):
                break

            assert (drs_objects[start].size == drs_objects_sign[i].size)
            drs_objects[start].access_methods = drs_objects_sign[i].access_methods
            i += 1

        new_objects = drs_objects[start - 9: start + 1]
        new_stuff = drs_manager.optimize_workload(new_objects)
        drs_manager.download(new_stuff, destination_dir)
        start = start + 1

        # show results
        if not silent:
            for drs_object in drs_objects:
                if len(drs_object.errors) > 0:
                    logger.error(
                        (drs_object.name, 'ERROR', drs_object.size, len(
                            drs_object.file_parts), drs_object.errors))
                else:
                    pass
                    logger.info((drs_object.name, 'OK', drs_object.size, len(drs_object.file_parts)))
            logger.info(('done', 'statistics.max_files_open', drs_client.statistics.max_files_open))

        for drs_object in drs_objects:
            at_least_one_error = False
            if len(drs_object.errors) > 0:
                logger.error((drs_object.name, 'ERROR', drs_object.size, len(drs_object.file_parts), drs_object.errors))
                at_least_one_error = True
        if at_least_one_error:
            exit(99)


def _extract_tsv_info(manifest_path: Path, drs_header: str) -> List[str]:
    """Extract the DRS URI's from the provided TSV file.

    Args:
        manifest_path (str): The input file containing a list of DRS URI's.
        drs_header (str): Column header for the DRS URI's.
    Returns:
        List[str]: The URI's corresponding to the DRS objects.
    """
    uris = []
    uri_index = 0
    header = None
    with open(Path(manifest_path)) as file:
        tsv_file = csv.reader(file, delimiter="\t")
        headers = next(tsv_file)
        # search for header name
        if drs_header is None:
            for col in headers:
                if ('uri' in col.lower()):
                    header = headers[headers.index(col)]
                    uri_index = headers.index(col)
                    break
        else:
            if (drs_header in headers):
                uri_index = headers.index(drs_header)
                header = drs_header

        # add url to urls list
        if (header is not None):
            for row in tsv_file:
                uris.append(row[uri_index])

        # add url to urls list
        if (header is not None):
            for row in tsv_file:
                uris.append(row[uri_index])
        else:
            raise KeyError(
                "Key format for drs_uri is bad. Make sure the column that contains the URIS has 'uri' somewhere in it,"
                "   or the URI header matches the uri header name in the TSV file that was specified")

        for url in uris:
            if ('/' in url):
                continue
            else:
                raise Exception(
                    "Check that your header name for your DRS URIS is directly above the column of your DRS URIS")
    return uris


if __name__ == "__main__":
    cli()
