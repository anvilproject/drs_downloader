import asyncio
import base64
import os.path
import shutil
import threading
from dataclasses import dataclass
from typing import List

import aiofiles
import aiohttp
from tempfile import TemporaryDirectory
from urllib.parse import urlparse
from drs_downloader.logger import logger
import hashlib
from pathlib import Path
from tqdm import tqdm


async def get_content_length(url):
    async with aiohttp.ClientSession() as session:
        async with session.head(url) as request:
            logger.debug(('head', url, request))
            return request.content_length


def parts_generator(size, part_size, start=0):
    while size - start > part_size:
        yield start, start + part_size
        start += part_size + 1
    yield start, size


async def async_download(url, headers, save_path):
    session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=None, sock_read=None)
    async with aiohttp.ClientSession(headers=headers, timeout=session_timeout,
                                     connector=aiohttp.TCPConnector(ssl=False)) as session:
        async with session.get(url) as request:
            logger.debug(('===> get', headers))
            file = await aiofiles.open(save_path, 'wb')
            await file.write(await request.content.read())
            logger.debug(('download', threading.get_ident(), save_path))
            return save_path


class Wrapped(object):
    """Wrap the read() method and calculate MD5"""

    def __init__(self, file_, md5_hash):
        self._file = file_
        self._md5_hash = md5_hash

    def read(self, size):
        buffer = self._file.read(size)
        self._md5_hash.update(buffer)
        return buffer

    def __getattr__(self, attr):
        return getattr(self._file, attr)


async def process(url, size, expected_md5, dest):
    logger.debug(('process', url))
    filename = os.path.basename(urlparse(url).path)
    tmp_dir = TemporaryDirectory(prefix=filename, dir=os.path.abspath('.'))
    # size = await get_content_length(url)
    tasks = []
    file_parts = []
    for number, sizes in enumerate(parts_generator(size)):
        part_file_name = os.path.join(tmp_dir.name, f'{filename}.part{number}')
        file_parts.append(part_file_name)
        tasks.append(async_download(url, {'Range': f'bytes={sizes[0]}-{sizes[1]}'}, part_file_name))
    await asyncio.gather(*tasks)
    with open(dest.joinpath(filename), 'wb') as wfd:
        md5_hash = hashlib.md5()
        for f in file_parts:
            with open(f, 'rb') as fd:
                wrapped_fd = Wrapped(fd, md5_hash)
                shutil.copyfileobj(wrapped_fd, wfd)
        actual_md5 = md5_hash.hexdigest()
        # compare calculated md5 vs expected
        assert expected_md5 == actual_md5, f"Actual md5 {actual_md5} does not match expected {expected_md5}"
        base64_md5 = base64.b64encode(bytes.fromhex(actual_md5))
        logger.debug(('md5', threading.get_ident(), filename, actual_md5, base64_md5))
    return True


@dataclass
class DownloadURL(object):
    """Information about the file to be downloaded."""
    url: str
    """Signed url."""
    md5: str
    """Needed for integrity check."""
    size: int
    """Needed for multi part download."""


async def _download(urls: List[DownloadURL], dest: Path):
    """Download urls."""
    results = await asyncio.gather(*[process(url.url, url.size, url.md5, dest) for url in tqdm(urls)])
    return results


def download(urls: List[DownloadURL], dest):
    """Setup async loop and download urls."""
    import time
    start_code = time.monotonic()
    logger.debug('START')
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results = loop.run_until_complete(_download(urls, dest))
    logger.info(f'{time.monotonic() - start_code} seconds {results}')
