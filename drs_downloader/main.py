import logging
import multiprocessing
from drs_downloader.download import async_download, parts_generator, Wrapped
from drs_downloader.logger import logger
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Iterator, Tuple
from urllib.parse import urlparse
import aiohttp
import asyncio
import base64
import click
import hashlib
import math
import os
import os.path
import shutil
import subprocess
import threading
import tqdm.asyncio
import csv

# Searches for URI. The key assumption is that URI is not used in any of the other columns.
# If this is not the case then there will be problems


def _extract_tsv_info(tsv_path: str, header: str) -> List[str]:
    """Extract the DRS URI's from the provided TSV file.

    Args:
        tsv_path (str): The input file containing a list of DRS URI's.
        drs_header (str): Column header for the DRS URI's.

    Returns:
        List[str]: The URI's corresponding to the DRS objects.
    """
    urls = []
    uri_index = 0
    with open(Path(tsv_path)) as file:
        tsv_file = csv.reader(file, delimiter="\t")
        headers = next(tsv_file)

        # search for header name
        if header is None:
            for col in headers:
                if ('uri' in col.lower()):
                    header = headers[headers.index(col)]
                    uri_index = headers.index(col)
                    break
        else:
            uri_index = headers.index(header)

        # add url to urls list
        if (header is not None):
            for row in tsv_file:
                urls.append(row[uri_index])
        else:
            raise KeyError("Key format for drs_uri is bad. Make sure the collumn \
                that contains the URIS has 'uri' somewhere in it")
    file.close()
    return urls


async def _sign_url(session, object_id: str, url_endpoint: str) -> Tuple[str, str, str, str]:
    data = '{ "url": "' + object_id + '", "fields": ["fileName", "size", "hashes", "accessUrl"]}'
    async with session.post(url=url_endpoint, data=data) as response:
        resp = await response.json(content_type=None)  # content_type=None
        return resp['accessUrl']['url'], resp['size'], resp['hashes']['md5'], resp['fileName']


def _get_auth_token() -> str:
    """Get Google Cloud authentication token.
    User must run 'gcloud auth login' from the shell before starting this script.

    Returns:
        str: auth token
    """
    token_command = "gcloud auth print-access-token"
    cmd = token_command.split(' ')
    token = subprocess.check_output(cmd).decode("ascii")[0:-1]
    return token


async def _run_sign_urls(object_ids: List[str], leave: bool) -> List[str]:
    tasks = []
    conn = aiohttp.TCPConnector(limit=10, ssl=False)
    url_endpoint = "https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v3"
    token = _get_auth_token()
    header = {'authorization': 'Bearer ' + token, 'content-type': 'application/json'}
    session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=None, sock_read=None)

    async with aiohttp.ClientSession(trust_env=True, headers=header, connector=conn,
                                     timeout=session_timeout) as session:

        for object_id in object_ids:
            task = asyncio.create_task(_sign_url(session=session, object_id=object_id, url_endpoint=url_endpoint))
            tasks.append(task)

        signed_urls = [
            await f
            for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks), leave=leave, desc="SigningProgress")
        ]
    return signed_urls


def _chunker(seq: Iterator, size: int) -> Iterator:
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def _sign_urls(object_ids: List[str], signers: int) -> List[str]:
    signed_urls_ = []
    total_batches = len(object_ids) / signers
    # if fractional
    if total_batches - round(total_batches) > 0:
        total_batches += 1

    current = 0
    for chunk_of_object_ids in tqdm.tqdm(_chunker(object_ids, signers), total=total_batches,
                                         desc="signing", disable=True):
        signed_urls_.extend(asyncio.run(_run_sign_urls(object_ids=chunk_of_object_ids,
                                                       leave=(current == total_batches))))
        current += 1
    return signed_urls_


def _download_urls(signed_urls: List[str], dest: Path, downloaders: int, parts: int) -> List[str]:

    total_batches = len(signed_urls) / downloaders
    if total_batches - round(total_batches) > 0:
        total_batches += 1

    current = 0
    paths = []

    for chunk_of_signed_urls in _chunker(signed_urls, downloaders):
        paths.extend(asyncio.run(_run_download(signed_urls=chunk_of_signed_urls, leave=(current == total_batches),
                                               dest=dest, parts=parts)))
        current += 1

    return paths


async def _run_download(signed_urls: List[str], leave: bool, dest: Path, parts: int) -> List[str]:
    tasks = []
    logger.debug("NUMBER OF SIGNED URLS TO BE DOWNLOADED ", len(signed_urls))
    for signed_url in signed_urls:
        task = asyncio.create_task(
            _run_download_parts(
                signed_url=signed_url,
                dest=dest,
                parts=parts))
        tasks.append(task)

    paths = [await f for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks),
                                        leave=leave, desc="Batch Total Progress")]
    return paths


# The TQDM progress bar and downloader like to see file parts that are
# rounded to the nearest MB/GB/KB so this is what this function does
def _round_magnitude(x: int):
    a = 0
    while x > 1000:
        a += 1  # This will go up the suffixes tuple with each division
        x = x / 1000
    return math.ceil(x) * 1024 ** a


async def _run_download_parts(signed_url: str, dest: Path, parts: int) -> List[str]:
    tasks = []
    file_parts = []

    filename = os.path.basename(urlparse(signed_url[0]).path)
    tmp_dir = TemporaryDirectory(prefix=filename, dir=os.path.abspath('.'))
    part_size = int(signed_url[1] / parts)
    part_size = _round_magnitude(part_size)
    # split each file into parts and download the parts in one temp folder.
    for number, sizes in enumerate(parts_generator(
            signed_url[1], part_size)):  # NUMBER_OF_PARTS_UPPER  as an additional argument
        part_file_name = os.path.join(tmp_dir.name, f'{filename}.part{number}')
        file_parts.append(part_file_name)
        task = asyncio.create_task(
            async_download(
                signed_url[0], {
                    'Range': f'bytes={sizes[0]}-{sizes[1]}'}, part_file_name))
        tasks.append(task)

    paths = [await f for f in tqdm.tqdm(asyncio.as_completed(tasks), total=(parts),
             leave=True, desc=(str(task.get_name())))]

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
    with open(dest.joinpath(filename), 'wb') as wfd:
        md5_hash = hashlib.md5()
        for f in file_parts:
            with open(f, 'rb') as fd:
                wrapped_fd = Wrapped(fd, md5_hash)
                shutil.copyfileobj(wrapped_fd, wfd)
        actual_md5 = md5_hash.hexdigest()
        base64_md5 = base64.b64encode(bytes.fromhex(actual_md5))
        logger.debug(('md5', threading.get_ident(), filename, actual_md5, base64_md5, file_parts))
        assert signed_url[2] == actual_md5, f"Actual md5 {actual_md5} does not match expected {signed_url[2]}"

    return paths


# downloaders * parts = total number of open files at one time.
# Keep this below your shell's max number of open files that it allows. You can find this value by typing
#  ulimit -n in your terminal or you can chage it by typing ulimit -n [new_number]
def download_files(tsv: str, header: str, dest: str, signers: int,
                   downloaders: int, parts: int):

    drs_ids = _extract_tsv_info(tsv, header)

    # as long as signers and downloaders are the same, there should never be an
    # issue with URLS expiring since the same number of URLS that are signed
    # are passed to the ASYNC dowloader. This would only potentially become an issue if
    # it takes >15 minuts to sign all of the URLS that are to be passed to the downloader.

    parts_two = float(len(drs_ids) / signers)

    if (parts_two % 1 != 0):
        parts_two = parts_two + 0

    parts_two = int(parts_two)
    start = 0
    if (parts_two == 0):
        parts_two = 1

    for i in tqdm.tqdm(range(parts_two), desc="total download progress"):
        signed_urls = _sign_urls(object_ids=drs_ids[start:start + downloaders], signers=signers)
        _download_urls(signed_urls=signed_urls, dest=Path(dest), downloaders=downloaders, parts=parts)
        start = start + downloaders


@click.command()
@click.option('--tsv', prompt='TSV file', help='The input TSV file. Example: terra-data.tsv')
@click.option('--header', help='The column header in the TSV file associated with the DRS URIs.'
              'Example: pfb:ga4gh_drs_uri')
@click.option('--dest', prompt='Download destination',
              help='The file path of the output file to download to. Relative or Absolute. Example: /tmp/DATA')
@click.option('--signers', default=10, show_default=True, help='The maximum number of files to be \
    signed at a time. If you are downloading files in the \
   GB this number should be the same as downloaders flag. If this variable is different than downloaders, you will run \
    into errors with files that take longer to download than 15 minutes')
@click.option('--downloaders', default=10, show_default=True, help='The maximum number of files to be\
     downloaded at a time. If you are downloading files in the \
  GB this  number should be the same  as signers flag')
@click.option('--parts', default=10, show_default=True, help='the maximum number of pieces a file should\
     be divided into to show progress. GB sized files should have >20 parts \
    MB sized files can have only one part.')
@click.option('--verbose', '-v', is_flag=True, help='Enable downloading and debugging output')
def main(tsv: str, header: str, dest: str, signers: int, downloaders: int, parts: int, verbose: bool):
    if (verbose):
        logger.setLevel(logging.DEBUG)

    logger.info('Welcome to the DRS Downloader!')
    logger.info(f'Beginning download to {dest}')
    download_files(tsv, header, dest, signers, downloaders, parts)
    logger.info('Downloading complete!')


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
