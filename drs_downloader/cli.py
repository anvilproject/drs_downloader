import logging
import multiprocessing
from pathlib import Path
from typing import List
import math
import tqdm
import click
import os
import csv
import sys
from sys import exit

from drs_downloader.clients.gen3 import Gen3DrsClient
from drs_downloader.clients.mock import MockDrsClient
from drs_downloader.clients.terra import TerraDrsClient
from drs_downloader.manager import DrsAsyncManager, DrsObject
from drs_downloader import check_for_AnVIL_URIS

from drs_downloader import DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS

logger = logging.getLogger()
file_logger = logging.getLogger("file_logger")

# Clear the logger file from the previous run


@click.group()
def cli():
    with open("drs_downloader.log", "w") as _:
        pass
    """Copy DRS objects from the cloud to your local system ."""
    pass


@cli.command()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    show_default=True,
    default=False,
    help="Display every logger",
)
@click.option(
    "--destination-dir",
    "-d",
    show_default=True,
    default=os.getcwd(),
    help="Destination directory.",
    required=True
)
@click.option("--manifest-path", "-m", show_default=True, help="Path to manifest tsv.")
@click.option(
    "--drs-column-name",
    default="ga4gh_drs_uri",
    show_default=True,
    help="The column header in the TSV file associated with the DRS URIs."
    "Example: pfb:ga4gh_drs_uri",
    required=True
)
@click.option(
    "--duplicate",
    default=False,
    is_flag=True,
    show_default=True,
    help="This flag is used to specify wether"
    "or not to download the file again if it already exists in the directory"
    "Example: True",
)
def mock(
    verbose: bool,
    destination_dir: str,
    manifest_path: str,
    drs_column_name: str,
    duplicate: bool,
):
    """Generate test files locally, without the need for server."""

    #
    # get ids from manifest
    ids_from_manifest = _extract_tsv_info(Path(manifest_path), drs_column_name)

    # perform downloads with a mock drs client
    _perform_downloads(
        destination_dir, MockDrsClient(), ids_from_manifest, user_project=None, verbose=verbose, duplicate=duplicate,
    )


@cli.command()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    show_default=True,
    default=False,
    help="Display every logger",
)
@click.option(
    "--destination-dir",
    "-d",
    show_default=True,
    default=os.getcwd(),
    help="Destination directory.",
    required=True
)
@click.option(
    "--manifest-path",
    "-m",
    show_default=True,
    help="Path to manifest tsv.",
    required=True
)
@click.option(
    "--drs-column-name",
    default=None,
    help="The column header in the TSV file associated with the DRS URIs."
    "Example: pfb:ga4gh_drs_uri",
)
@click.option(
    "--user-project", "-u",
    default=None,
    show_default=True,
    help="This option is used to specify the Terra Workspace"
         "Google project id if the requester is paying for the download",
)
@click.option(
    "--duplicate",
    default=False,
    is_flag=True,
    show_default=True,
    help="This flag is used to specify wether"
    "or not to download the file again if it already exists in the directory"
    "Example: True",
)
@click.option(
    "--string-mode",
    default=None,
    show_default=True,
    help="This option is used when you want to run the downloader with URIS"
         "that you provide in string form with uris seperated by commas the command line. ex: 'uri1, uri2, uri3'",
)
def terra(
    verbose: bool,
    destination_dir: str,
    manifest_path: str,
    drs_column_name: str,
    user_project: str,
    duplicate: bool,
    string_mode: str,
):
    """Copy files from terra.bio"""

    # get ids from manifest
    if string_mode is not None:
        ids_from_manifest = string_mode.split(",")
        ids_from_manifest = [s.replace(" ", "") for s in ids_from_manifest]
    else:
        ids_from_manifest = _extract_tsv_info(Path(manifest_path), drs_column_name)

    Contains_AnVIL_Uris = check_for_AnVIL_URIS(ids_from_manifest)
    if Contains_AnVIL_Uris and not user_project:
        file_logger.error(
            ("ERROR: AnVIL Drs URIS starting with  'drs://drs.anv0:' or 'drs://dg.anv0: were"
             "provided in the manifest but no Terra workspace Google project id was given. Specify one with"
             "the --user-project option")
        )
        logger.error(
            ("ERROR: AnVIL Drs URIS starting with  'drs://drs.anv0:' or 'drs://dg.anv0: were"
             "provided in the manifest but no Terra workspace Google project id was given. Specify one with"
             "the --user-project option")
        )
        exit(1)

    # perform downloads with a terra drs client
    _perform_downloads(
        destination_dir,
        TerraDrsClient(),
        ids_from_manifest=ids_from_manifest,
        user_project=user_project,
        verbose=verbose,
        duplicate=duplicate,

    )


@cli.command()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    show_default=True,
    default=False,
    help="Display every logger",
)
@click.option(
    "--destination-dir",
    "-d",
    show_default=True,
    default=os.getcwd(),
    help="Destination directory.",
    required=True
)
@click.option(
    "--manifest-path",
    "-m",
    show_default=True,
    help="Path to manifest tsv.",
    required=True
)
@click.option(
    "--drs-column-name",
    default="ga4gh_drs_uri",
    show_default=True,
    help="The column header in the TSV file associated with the DRS URIs."
         "Example: pfb:ga4gh_drs_uri",
)
@click.option(
    "--api-key-path",
    show_default=True,
    help="Gen3 credentials file"
)
@click.option("--endpoint", show_default=True, required=True, help="Gen3 endpoint")
@click.option(
    "--duplicate",
    default=False,
    is_flag=True,
    show_default=True,
    help="This flag is used to specify wether"
         "or not to download the file again if it already exists in the directory"
         "Example: True",
)
def gen3(
    verbose: bool,
    destination_dir: str,
    manifest_path: str,
    drs_column_name: str,
    api_key_path: str,
    endpoint: str,
    duplicate: bool,
):
    """Copy files from gen3 server."""
    # read from manifest
    assert api_key_path is not None, "If using gen3 mode an api key path must be provided with --api-key-path"
    ids_from_manifest = _extract_tsv_info(Path(manifest_path), drs_column_name)

    _perform_downloads(
        destination_dir,
        Gen3DrsClient(api_key_path=api_key_path, endpoint=endpoint),
        ids_from_manifest,
        verbose=verbose,
        duplicate=duplicate,
        user_project=None
    )


# CREDIT https://stackoverflow.com/questions/5194057/better-way-to-convert-file-sizes-in-python
def pretty_size(bytes):
    """Integer -> human readable Data Size Pretty Printer"""
    assert (bytes and bytes > 0), f"ERROR, The total download size {bytes} is Zero or None"
    units = [
        (1 << 50, " PB"),
        (1 << 40, " TB"),
        (1 << 30, " GB"),
        (1 << 20, " MB"),
        (1 << 10, " KB"),
        (1, (" bytes")),
    ]
    if (bytes / 1000000000 < 1):
        price = 0.1
    else:
        price = '%.2f' % ((bytes / 1000000000) * 0.1)

    for factor, suffix in units:
        if bytes >= factor:
            break
    amount = int(bytes / factor)
    return str(amount) + suffix, price


def _end_routine(drs_client: TerraDrsClient, drs_objects: List[DrsObject], verbose: bool):
    at_least_one_error = False
    oks = 0
    for drs_object in drs_objects:
        if len(drs_object.errors) == 0:
            file_logger.info(
                (drs_object.name, "OK", drs_object.size, len(drs_object.file_parts))
            )
            logger.info(
                (drs_object.name, "OK", drs_object.size, len(drs_object.file_parts))
            )
            oks += 1

        file_logger.info(('done', 'statistics.max_files_open', drs_client.statistics.max_files_open))
        if verbose:
            logger.info(('done', 'statistics.max_files_open', drs_client.statistics.max_files_open))
    file_logger.info("%s/%s files have downloaded successfully", oks, len(drs_objects))
    logger.info("%s/%s files have downloaded successfully", oks, len(drs_objects))

    for drs_object in drs_objects:
        if len(drs_object.errors) > 0:
            file_logger.error(
                (
                    drs_object.name,
                    "ERROR",
                    drs_object.size,
                    len(drs_object.file_parts),
                    drs_object.errors,
                )
            )
            logger.error(
                (
                    drs_object.name,
                    "ERROR",
                    drs_object.size,
                    len(drs_object.file_parts),
                    drs_object.errors,
                )
            )
            at_least_one_error = True

    if at_least_one_error:
        exit(1)


def _perform_downloads(
    destination_dir, drs_client, ids_from_manifest, user_project: str, verbose: bool, duplicate: bool
):
    """Common helper method to run downloads."""

    try:
        if destination_dir:
            destination_dir = Path(destination_dir)
            if not os.path.exists(destination_dir):
                destination_dir.mkdir(parents=True, exist_ok=True)
    except BaseException as e:
        file_logger.error(f"Invalid --destination-dir path provided: {e}")
        logger.error(f"Invalid --destination-dir path provided: {e}")
        exit(1)

    file_logger.info(f"Downloading to: {destination_dir.resolve()}")
    logger.info(f"Downloading to: {destination_dir.resolve()}")

    # create a manager
    drs_manager = DrsAsyncManager(drs_client=drs_client, show_progress=not verbose)

    # call the server, get size, checksums etc.; sort them by size
    drs_objects = drs_manager.get_objects(ids_from_manifest, verbose=verbose)

    file_logger.info(f"Drs Objects after get_objects function {drs_objects}")

    # If every object has an error exit early since these early errors are not recoverable
    if all(len(obj.errors) > 0 for obj in drs_objects):
        file_logger.error("every single object recieved an\
error in git objects function, so starting end routine early")
        logger.error("every single object recieved an error in git objects function, so starting end routine early")
        _end_routine(drs_client, drs_objects, verbose)

    # there are many reasons why this exception gets caught and many of them don't have
    # much to do with the object's size, but things that happen along the way
    total_size_list = [total.size for total in drs_objects]
    assert (sum(total_size_list) > 0), (
        logger.error("FATAL ERROR: No size data was returned from get_objects.\
 Check your uris to make sure that they are properly formatted"),
        file_logger.error("FATAL ERROR: No size data was returned from get_objects.\
 Check your uris to make sure that they are properly formatted"),
        exit())

    total, price = pretty_size(sum(total_size_list))
    file_logger.info(f"Total download size is {total}")
    file_logger.info(f"Estimated download cost is ${price}")
    logger.info(f"Total download size is {total}")
    logger.info(f"Estimated download cost is ${price}")

    # sorting by size here also moves errored out size 0 objects to the top so that they can be batched up
    # together and skipped before the actual downloading starts
    drs_objects.sort(key=lambda x: x.size, reverse=False)
    # optimize based on workload
    drs_objects = drs_manager.optimize_workload(verbose, drs_objects)

    file_logger.info(f"Drs Objects after optimize_workload function {drs_objects}")
    if verbose:
        logger.info(f"Drs Objects after optimize_workload function {drs_objects}")

    # determine the total number of batches
    total_batches = len(drs_objects) / DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS
    if math.ceil(total_batches) - total_batches > 0:
        total_batches += 1
        total_batches = int(total_batches)

    progress_bar = tqdm.tqdm(
        DrsAsyncManager.chunker(drs_objects, DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS),
        total=total_batches,
        desc="TOTAL_DOWNLOAD_PROGRESS",
        leave=False,
        file=sys.stdout,
        disable=(total_batches == 1),
    )

    for chunk_of_drs_objects in progress_bar:
        file_logger.info(str(progress_bar))
        if all(len(obj.errors) > 0 for obj in chunk_of_drs_objects):
            file_logger.warning(f"Every object in the batch has an error so \
skipping downloading for objects {chunk_of_drs_objects}")
            if verbose:
                logger.warning(f"Every object in the batch has an error so \
skipping downloading for objects {chunk_of_drs_objects}")
            continue
        # the scenario where some

        drs_manager.download(chunk_of_drs_objects, destination_dir, user_project=user_project,
                             duplicate=duplicate, verbose=verbose)

    _end_routine(drs_client, drs_objects, verbose)


def _extract_tsv_info(manifest_path: Path, drs_header: str) -> List[str]:
    """Extract the DRS URI's from the provided TSV file.

    Args:
        manifest_path (str): The input file containing a list of DRS URI's.
        drs_header (str): Column header for the DRS URI's.
    Returns:
        List[str]: The URI's corresponding to the DRS objects.
    """
    assert (
        manifest_path.is_file()
    ), "The manifest file path and name given does not exist"

    uris = []
    uri_index = 0
    header = None
    with open(Path(manifest_path)) as file:
        tsv_file = csv.reader(file, delimiter="\t")
        headers = next(tsv_file)
        # search for header name
        if drs_header is None:
            for col in headers:
                if "uri" in col.lower():
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
                # solves an issue where blank lines would be read from the TSV
                if row[uri_index] == '':
                    continue
                uris.append(row[uri_index])

        else:
            raise KeyError(
                f"DRS header value '{drs_header}' not found in manifest file {manifest_path}."
                " Please specify a new value with the --drs-column-name flag."
            )

        for url in uris:
            if "drs://" in url or "DRS://" in url:
                continue
            else:
                raise Exception(
                    "Check that your header name for your DRS URIS is directly above the column of your DRS URIS"
                )

        if len(uris) != len(set(uris)):
            raise Exception(
                "Duplicate URIS: ",
                str(list(set([x for x in uris if uris.count(x) > 1]))),
                "found in your TSV file ",
            )

    return uris


if __name__ == "__main__":
    multiprocessing.freeze_support()
    cli()
