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
from drs_downloader.manager import DrsAsyncManager

from drs_downloader import DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS

logger = logging.getLogger()  # these control our simulation

with open("drs_downloader.log", "w") as fd:
    pass


@click.group()
def cli():
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
)
@click.option("--manifest-path", "-m", show_default=True, help="Path to manifest tsv.")
@click.option(
    "--drs-column-name",
    default="ga4gh_drs_uri",
    show_default=True,
    help="The column header in the TSV file associated with the DRS URIs."
    "Example: pfb:ga4gh_drs_uri",
)
@click.option(
    "--duplicate",
    default=False,
    is_flag=True,
    show_default=True,
    help="This flag is used to specify wether \
    or not to download the file again if it already exists in the directory"
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
        destination_dir, MockDrsClient(), ids_from_manifest, verbose, duplicate=duplicate
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
)
@click.option("--manifest-path", "-m", show_default=True, help="Path to manifest tsv.")
@click.option(
    "--drs-column-name",
    default=None,
    help="The column header in the TSV file associated with the DRS URIs."
    "Example: pfb:ga4gh_drs_uri",
)
@click.option(
    "--duplicate",
    default=False,
    is_flag=True,
    show_default=True,
    help="This flag is used to specify wether \
    or not to download the file again if it already exists in the directory"
    "Example: True",
)
def terra(
    verbose: bool,
    destination_dir: str,
    manifest_path: str,
    drs_column_name: str,
    duplicate: bool,
):
    """Copy files from terra.bio"""

    # get ids from manifest
    ids_from_manifest = _extract_tsv_info(Path(manifest_path), drs_column_name)

    # perform downloads with a terra drs client
    _perform_downloads(
        destination_dir,
        TerraDrsClient(),
        ids_from_manifest=ids_from_manifest,
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
)
@click.option("--manifest-path", "-m", show_default=True, help="Path to manifest tsv.")
@click.option(
    "--drs-column-name",
    default="ga4gh_drs_uri",
    show_default=True,
    help="The column header in the TSV file associated with the DRS URIs."
    "Example: pfb:ga4gh_drs_uri",
)
@click.option("--api-key-path", show_default=True, help="Gen3 credentials file")
@click.option("--endpoint", show_default=True, required=True, help="Gen3 endpoint")
@click.option(
    "--duplicate",
    default=False,
    is_flag=True,
    show_default=True,
    help="This flag is used to specify wether \
    or not to download the file again if it already exists in the directory"
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
    ids_from_manifest = _extract_tsv_info(Path(manifest_path), drs_column_name)

    _perform_downloads(
        destination_dir,
        Gen3DrsClient(api_key_path=api_key_path, endpoint=endpoint),
        ids_from_manifest,
        verbose,
        duplicate=duplicate,
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
    if (bytes/1000000000 < 1):
        price = 0.1
    else:
        price = '%.2f' % ((bytes/1000000000) * 0.1)

    for factor, suffix in units:
        if bytes >= factor:
            break
    amount = int(bytes / factor)
    return str(amount) + suffix, price


def _perform_downloads(
    destination_dir, drs_client, ids_from_manifest, verbose: bool, duplicate: bool
):
    """Common helper method to run downloads."""

    # verify parameters
    if destination_dir:
        destination_dir = Path(destination_dir)
        destination_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Downloading to: {destination_dir.resolve()}")

    # create a manager
    drs_manager = DrsAsyncManager(drs_client=drs_client, show_progress=not verbose)

    # call the server, get size, checksums etc.; sort them by size
    drs_objects = drs_manager.get_objects(ids_from_manifest, verbose=verbose)
    total_size_list = [total.size for total in drs_objects]
    assert (sum(total_size_list) > 0), (logger.error("FATAL ERROR: No size data was returned from get_objects.\
 Check your uris to make sure that they are properly formatted"), exit())

    total, price = pretty_size(sum(total_size_list))
    logger.info(f"Total download size is {total}")
    logger.info(f"Estimated download cost is ${price}")

    drs_objects.sort(key=lambda x: x.size, reverse=False)
    # optimize based on workload
    drs_objects = drs_manager.optimize_workload(verbose, drs_objects)
    # determine the total number of batches
    total_batches = len(drs_objects) / DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS
    if math.ceil(total_batches) - total_batches > 0:
        total_batches += 1
        total_batches = int(total_batches)
    for chunk_of_drs_objects in tqdm.tqdm(
        DrsAsyncManager.chunker(drs_objects, DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS),
        total=total_batches,
        desc="TOTAL_DOWNLOAD_PROGRESS",
        leave=False,
        file=sys.stdout,
        disable=(total_batches == 1),
    ):

        drs_manager.download(chunk_of_drs_objects, destination_dir, duplicate=duplicate, verbose=verbose)

    at_least_one_error = False
    oks = 0
    for drs_object in drs_objects:
        if len(drs_object.errors) == 0:
            if not verbose:
                logger.info(
                    (drs_object.name, "OK", drs_object.size, len(drs_object.file_parts))
                )
            oks += 1

        if verbose:
            logger.info(('done', 'statistics.max_files_open', drs_client.statistics.max_files_open))
    logger.info("%s/%s files have downloaded successfully", oks, len(drs_objects))

    for drs_object in drs_objects:
        if len(drs_object.errors) > 0:
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
                uris.append(row[uri_index])

        else:
            raise KeyError(
                f"DRS header value '{drs_header}' not found in manifest file {manifest_path}."
                " Please specify a new value with the --drs-column-name flag."
            )

        for url in uris:
            if "drs://" in url:
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
