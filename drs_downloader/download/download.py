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
import sys
from urllib.parse import urlparse
import hashlib
import logging

# logging.basicConfig(format="%(processName)s %(threadName)s: %(message)s", encoding='utf-8', level=logging.DEBUG)


def _logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    return logger


async def get_content_length(url):
    async with aiohttp.ClientSession() as session:
        async with session.head(url) as request:
            _logger(__name__).debug(('head', url, request))
            return request.content_length


def parts_generator(size, start=0, part_size=10 * 1024 ** 2):
    while size - start > part_size:
        yield start, start + part_size
        start += part_size
    yield start, size


async def async_download(url, headers, save_path):
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url) as request:
            _logger(__name__).debug(('get', url, request))
            file = await aiofiles.open(save_path, 'wb')
            await file.write(await request.content.read())
            _logger(__name__).debug(('download', threading.get_ident(), save_path))


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


async def process(url, size, expected_md5):
    _logger(__name__).debug(('process', url))
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
    with open(filename, 'wb') as wfd:
        md5_hash = hashlib.md5()
        for f in file_parts:
            with open(f, 'rb') as fd:
                wrapped_fd = Wrapped(fd, md5_hash)
                shutil.copyfileobj(wrapped_fd, wfd)
        actual_md5 = md5_hash.hexdigest()
        # compare calculated md5 vs expected
        assert expected_md5 == actual_md5, f"Actual md5 {actual_md5} does not match expected {expected_md5}"
        base64_md5 = base64.b64encode(bytes.fromhex(actual_md5))
        _logger(__name__).debug(('md5', threading.get_ident(), filename, actual_md5, base64_md5))
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


async def _download(urls: List[DownloadURL]):
    """Download urls."""
    results = await asyncio.gather(*[process(url.url, url.size, url.md5) for url in urls])
    return results


def download(urls: List[DownloadURL]):
    """Setup async loop and download urls."""
    import time
    start_code = time.monotonic()
    _logger(__name__).debug('START')
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(_download(urls))
    _logger(__name__).info(f'{time.monotonic() - start_code} seconds {results}')


# async def main():
#     # if len(sys.argv) <= 1:
#     #     print('Add URLS')
#     #     exit(1)
#     # urls = sys.argv[1:]
#     _logger(__name__).debug('BEFORE GATHER')
#     # ./etl/file --gen3_credentials_file credentials-aced-training-local.json  get-index --did 40be30f4-4a21-47f1-ab64-745bf04cadd5
#     urls = [ 'https://minio-default.compbio.ohsu.edu/aced-default/40be30f4-4a21-47f1-ab64-745bf04cadd5/tmp/DATA/758278b648?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test%2F20220922%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20220922T211915Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&client_id=&user_id=1&username=test&X-Amz-Signature=5a5b83cc97dd372cee50c44331ead025c9eca8f0638c405ec228c9f3be590b3c']
#     md5 = 'b7c91b3c87cded71bdcd2684cdb268f7'
#     size = 10794
#     await asyncio.gather(*[process(url, size, md5) for url in urls])
#
#
# if __name__ == '__main__':
#     import time
#
#     start_code = time.monotonic()
#     _logger(__name__).debug('START')
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(main())
#     _logger(__name__).info(f'{time.monotonic() - start_code} seconds!')
