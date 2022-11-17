from drs_downloader.download import DownloadURL, async_download, parts_generator, Wrapped
from drs_downloader.logger import logger
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Iterator
from urllib.parse import urlparse
import aiohttp
import asyncio
import base64
import click
import csv
import hashlib
import logging
import math
import multiprocessing
import os
import os.path
import shutil
import subprocess
import threading
import tqdm.asyncio


def _extract_tsv_info(tsv_path: str, header: str = "") -> List[str]:
    """Extracts the DRS URI's from the provided TSV file.
    Searches for URI. The key assumption is that URI is not used in any of the other columns.
    If this is not the case then there will be problems.

    Args:
        tsv_path (str): The input file containing a list of DRS URI's.
        header (str, optional): Column header for the DRS URI's. Defaults to "".

    Returns:
        List[str]: The URI's corresponding to the DRS objects.
    """
    urls = []
    uri_index = None
    tsv_file = None
    with open(Path(tsv_path)) as file:
        tsv_file = csv.reader(file, delimiter="\t")

        headers = next(tsv_file)

        # No header passed in, searching for 'uri' in file headers
        if header == "":
            for col in headers:
                if "uri" in col.lower():
                    uri_index = headers.index(col)
                    break
            if uri_index is None:
                logger.error("DRS URI column not found. Make sure the the URI column header contains 'uri'.")
                raise ValueError

        # User pass in header, try using that value
        else:
            try:
                uri_index = headers.index(header)
            except ValueError:
                logger.error(f"Header '{header}' not found in {tsv_path}")
                raise

        assert uri_index is not None

        # add url to urls list
        for row in tsv_file:
            urls.append(row[uri_index])

        return urls


async def _sign_url(session: aiohttp.ClientSession, object_id: str, url_endpoint: str) -> DownloadURL:
    """Returns signed URLs with corresponding md5 hash and file size.

    Args:
        session (aiohttp.ClientSession): the async aiohttp client session with the DRS server.
        object_id (str): the DRS URI to retrieve the signed URL for.
        url_endpoint (str): the URL for the DRS server.

    Returns:
        DownloadURL: the object consisting of the signed URL, md5 hash, and file size.
    """
    data = '{ "url": "' + object_id + '", "fields": ["fileName", "size", "hashes", "accessUrl"]}'
    async with session.post(url=url_endpoint, data=data) as response:
        resp = await response.json(content_type=None)  # content_type=None
        url = resp["accessUrl"]["url"]
        md5 = resp["hashes"]["md5"]
        size = resp["size"]
        downloadURL = DownloadURL(url, md5, size)
        return downloadURL


def _get_auth_token() -> str:
    """Returns Google Cloud authentication token.
    User must run 'gcloud auth login' from the shell before starting this script.

    Returns:
        str: Google auth token
    """
    token_command = "gcloud auth print-access-token"
    cmd = token_command.split(" ")
    token = subprocess.check_output(cmd).decode("ascii")[0:-1]
    return token


async def _run_sign_urls(object_ids: List[str], leave: bool) -> List[DownloadURL]:
    """Runs the async signer and outputs progress bars.

    Args:
        object_ids (List[str]): The DRS URIs to retrieve signed URLs for
        leave (bool): For tqdm, if true keeps all traces of the progressbar in terminal after download completes

    Returns:
        List[DownloadURL]: the signed URL, md5 hash, and file size for each DRS object
    """
    tasks = []
    conn = aiohttp.TCPConnector(limit=10, ssl=False)
    url_endpoint = "https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v3"
    token = _get_auth_token()
    header = {
        "authorization": "Bearer " + token,
        "content-type": "application/json"
    }
    session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=None, sock_read=None)

    async with aiohttp.ClientSession(
        trust_env=True, headers=header, connector=conn, timeout=session_timeout
    ) as session:
        for object_id in object_ids:
            task = asyncio.create_task(_sign_url(session=session, object_id=object_id, url_endpoint=url_endpoint))
            tasks.append(task)

        signed_urls = [
            await f
            for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks), leave=leave, desc="SigningProgress")
        ]
    return signed_urls


def _chunker(seq: Iterator, size: int) -> Iterator:
    return (seq[pos: pos + size] for pos in range(0, len(seq), size))


def _sign_urls(object_ids: List[str], signers: int) -> List[DownloadURL]:
    """Returns a list of signed downloadable URLs with corresponding file size and md5 hashes.

    Args:
        object_ids (List[str]): The DRS URIs to retrieve signed URLs for
        signers (int): The number of concurrent signers

    Returns:
        List[DownloadURL]: the signed URL, md5 hash, and file size for each DRS object
    """
    signed_urls = []
    total_batches = len(object_ids) / signers
    # if fractional
    if total_batches - round(total_batches) > 0:
        total_batches += 1

    current = 0
    for chunk_of_object_ids in tqdm.tqdm(
        _chunker(object_ids, signers), total=total_batches, desc="signing", disable=True
    ):
        signed_urls.extend(
            asyncio.run(_run_sign_urls(object_ids=chunk_of_object_ids, leave=(current == total_batches)))
        )
        current += 1
    return signed_urls


def _download_urls(signed_urls: List[DownloadURL], dest: Path, downloaders: int, parts: int) -> List[str]:
    """Downloads a series of files based of a list of signed URLs.

    Args:
        signed_urls (List[DownloadURL]): Downloadable URLs to fetch
        dest (Path): Download destination on local filesystem. Example: /tmp/DATA
        downloaders (int): Number of concurrent downloads to spawn
        parts (int): Number of parts of a given file to download concurrently

    Returns:
        List[str]: The downloaded file paths.
    """
    total_batches = len(signed_urls) / downloaders
    if total_batches - round(total_batches) > 0:
        total_batches += 1

    current = 0
    paths = []

    for chunk_of_signed_urls in _chunker(signed_urls, downloaders):
        paths.extend(
            asyncio.run(
                _run_download(
                    signed_urls=chunk_of_signed_urls, leave=(current == total_batches), dest=dest, parts=parts
                )
            )
        )
        current += 1

    return paths


async def _run_download(signed_urls: List[DownloadURL], leave: bool, dest: Path, parts: int) -> List[str]:
    """Runs the async downloader and outputs progress bars.

    Args:
        signed_urls (List[DownloadURL]): The list of signed URLs to download
        leave (bool): For tqdm, if true keeps all traces of the progressbar in terminal after download completes
        dest (Path): the download destination
        parts (int): the number of parts to divide the files into for download

    Returns:
        List[str]: The paths of the downloaded files
    """
    tasks = []
    logger.debug("NUMBER OF SIGNED URLS TO BE DOWNLOADED ", len(signed_urls))
    for signed_url in signed_urls:
        task = asyncio.create_task(_run_download_parts(signed_url=signed_url, dest=dest, parts=parts))
        tasks.append(task)

    paths = [
        await f
        for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks), leave=leave, desc="Batch Total Progress")
    ]
    return paths


def _round_magnitude(bytes: int) -> int:
    """The TQDM progress bar and downloader like to see file parts that are rounded
    to the nearest KB/MB/GB so this is what this function does.

    Args:
        bytes (int): number of raw bytes

    Returns:
        int: number of bytes as KB, MB, or GB
    """

    a = 0
    while bytes > 1000:
        a += 1  # This will go up the KB/MB/GB suffixes tuple with each division
        bytes = int(bytes / 1000)
    return math.ceil(bytes) * 1024**a


async def _run_download_parts(signed_url: DownloadURL, dest: Path, parts: int) -> List[str]:
    """Downloads a given DRS object by splitting up the file into parts and downloading each part concurrently.

    Args:
        signed_url (DownloadURL): the object consisting of the signed URL, md5 hash, and file size
        dest (Path): the download destination
        parts (int): the number of parts to divide the files into for download

    Returns:
        List[str]: The paths of the downloaded files
    """
    tasks = []
    file_parts = []

    filename = os.path.basename(urlparse(signed_url.url).path)
    tmp_dir = TemporaryDirectory(prefix=filename, dir=os.path.abspath("."))
    part_size = int(signed_url.size / parts)
    part_size = _round_magnitude(part_size)
    # split each file into parts and download the parts in one temp folder.
    for number, sizes in enumerate(
        parts_generator(signed_url.size, part_size)
    ):  # NUMBER_OF_PARTS_UPPER  as an additional argument
        part_file_name = os.path.join(tmp_dir.name, f"{filename}.part{number}")
        file_parts.append(part_file_name)
        task = asyncio.create_task(
            async_download(signed_url.url, {"Range": f"bytes={sizes[0]}-{sizes[1]}"}, part_file_name)
        )
        tasks.append(task)

    paths = [await f for f in tqdm.tqdm(asyncio.as_completed(tasks), total=(parts), leave=True, desc=(task.get_name()))]

    # paths.extend(paths)
    i = 1
    # Deal with duplicate file names by searching the directory for duplicate file names before writing to it.
    # This could still be an issue if There are duplicate file names in the same download batch that are
    # checking the same location that haven't been downloaded to yet at the same tim
    original_file_name = filename
    while True:
        if os.path.isfile(dest.joinpath(filename)):
            filename = f"{original_file_name}({i})"
            i = i + 1
            continue
        break

    # Write all of the file parts to the destination folder and verify the MD5 Hash of each file part
    dest.mkdir(parents=True, exist_ok=True)
    with open(dest.joinpath(filename), "wb") as wfd:
        md5_hash = hashlib.md5()
        for f in file_parts:
            with open(f, "rb") as fd:
                wrapped_fd = Wrapped(fd, md5_hash)
                shutil.copyfileobj(wrapped_fd, wfd)
        actual_md5 = md5_hash.hexdigest()
        base64_md5 = base64.b64encode(bytes.fromhex(actual_md5))
        logger.debug(("md5", threading.get_ident(), filename, actual_md5, base64_md5, file_parts))
        assert signed_url.md5 == actual_md5, f"Actual md5 {actual_md5} does not match expected {signed_url.md5}"

    return paths


def download_files(tsv: str, dest: str, header: str = "", signers: int = 10, downloaders: int = 10, parts: int = 10):
    """Driver function for file download.
    downloaders * parts = total number of open files at one time.
    Keep this below your shell's max number of open files that it allows. You can find this value by typing
    ulimit -n in your terminal or you can change it by typing ulimit -n <new_number>

    As long as signers and downloaders are the same, there should never be an issue with URLS expiring since the
    same number of URLS that are signed are passed to the async downloader. This would only potentially become an
    issue if it takes >15 minutes to sign all of the URLS that are to be passed to the downloader.

    Args:
        tsv (str): Input TSV file. Example: terra-data.tsv
        dest (str): Download destination. Example: /tmp/DATA
        header (str, optional): The DRS URI column in the TSV file. Example: pfb:ga4gh_drs_uri. Defaults to "".
        signers (int, optional): The maximum number of files to be signed at a time. Defaults to 10.
        downloaders (int, optional): The maximum number of files to be downloaded at a time. Defaults to 10.
        parts (int, optional): The maximum number of pieces a file should be divided into for download. Defaults to 10.
    """

    drs_ids = _extract_tsv_info(tsv, header)

    parts_two = float(len(drs_ids) / signers)

    if parts_two % 1 != 0:
        parts_two = parts_two + 0

    parts_two = int(parts_two)
    start = 0
    if parts_two == 0:
        parts_two = 1

    for i in tqdm.tqdm(range(parts_two), desc="total download progress"):
        signed_urls = _sign_urls(object_ids=drs_ids[start: start + downloaders], signers=signers)
        _download_urls(signed_urls=signed_urls, dest=Path(dest), downloaders=downloaders, parts=parts)
        start = start + downloaders


@click.command()
@click.option("--tsv", prompt="TSV file", help="The input TSV file. Example: terra-data.tsv")
@click.option(
    "--header", help="The column header in the TSV file associated with the DRS URIs. Example: pfb:ga4gh_drs_uri"
)
@click.option(
    "--dest",
    prompt="Download destination",
    help="The file path of the output file to download to. Relative or Absolute. Example: /tmp/DATA",
)
@click.option(
    "--signers",
    default=10,
    show_default=True,
    help=(
        "The maximum number of files to be signed at a time. If you are downloading files in the GB this number should"
        " be the same as downloaders flag. If this variable is different than downloaders, you will run into errors"
        " with files that take longer to download than 15 minutes"
    ),
)
@click.option(
    "--downloaders",
    default=10,
    show_default=True,
    help=(
        "The maximum number of files to be downloaded at a time. If you are downloading files in the GB this number"
        " should be the same  as signers flag"
    ),
)
@click.option(
    "--parts",
    default=10,
    show_default=True,
    help=(
        "The maximum number of pieces a file should be divided into to show progress. GB sized files should have >20"
        " parts MB sized files can have only one part."
    ),
)
@click.option("--verbose", "-v", is_flag=True, help="Enable downloading and debugging output")
def main(tsv: str, header: str, dest: str, signers: int, downloaders: int, parts: int, verbose: bool):
    if verbose:
        logger.setLevel(logging.DEBUG)

    logger.info("Welcome to the DRS Downloader!")
    logger.info(f"Beginning download to {dest}")
    download_files(tsv, dest, header, signers, downloaders, parts)
    logger.info("Downloading complete!")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
