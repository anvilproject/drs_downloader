import asyncio
import hashlib
import logging
import math
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Iterator, Tuple, Collection
import os
import tqdm
import tqdm.asyncio


from drs_downloader import DEFAULT_MAX_SIMULTANEOUS_OBJECT_RETRIEVERS, DEFAULT_MAX_SIMULTANEOUS_PART_HANDLERS, \
    DEFAULT_MAX_SIMULTANEOUS_DOWNLOADERS, DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS, DEFAULT_PART_SIZE, MB, GB

from drs_downloader.models import DrsClient, DrsObject

logger = logging.getLogger(__name__)


class DrsManager(ABC):
    """Manage DRSClient workload.

    """

    @abstractmethod
    def __init__(self, drs_client: DrsClient):
        self._drs_client = drs_client

    @abstractmethod
    def get_objects(self, object_ids: List[str]) -> List[DrsObject]:
        """Fetch list of DRSObject from passed ids.

        Args:
            object_ids: list of objects to fetch
        """
        pass

    @abstractmethod
    def download(self, drs_objects: List[DrsObject], destination_path: Path) -> List[DrsObject]:
        """Split the drs_objects into manageable parts, download the files.

        Args:
            drs_objects: objects to download
            destination_path: directory where to write files when complete

        Returns:
            list of updated DrsObjects
        """

    @abstractmethod
    async def optimize_workload(self, drs_objects: List[DrsObject]) -> List[DrsObject]:
        """
        Optimize the workload, sort prioritize and set thread management parameters.
        Args:
            drs_objects:

        Returns:

        """
        # TODO - now that we have the objects to download, we have an opportunity to shape the downloads
        # TODO - e.g. smallest files first?  tweak MAX_* to optimize per workload
        return drs_objects


class Wrapped(object):

    def __init__(self, file, hash_method):
        """
        Wrap the read() method and calculate hash
        Args:
            file: destination file
            hash_method: instantiated hash_method
        """
        self._file = file
        self._hash_method = hash_method

    def read(self, size):
        buffer = self._file.read(size)
        self._hash_method.update(buffer)
        return buffer

    def __getattr__(self, attr):
        return getattr(self._file, attr)


class DrsAsyncManager(DrsManager):
    """Manage DRSClient workload with asyncio threads, display progress.

    """

    def __init__(self, drs_client: DrsClient,
                 show_progress: bool = True,
                 part_size: int = DEFAULT_PART_SIZE,
                 max_simultaneous_object_retrievers=DEFAULT_MAX_SIMULTANEOUS_OBJECT_RETRIEVERS,
                 max_simultaneous_downloaders=DEFAULT_MAX_SIMULTANEOUS_DOWNLOADERS,
                 max_simultaneous_part_handlers=DEFAULT_MAX_SIMULTANEOUS_PART_HANDLERS,
                 max_simultaneous_object_signers=DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS):
        """

        Args:
            drs_client: the client that will interact with server
            show_progress: show progress bars
            part_size: tweak to optimize workload
            max_simultaneous_object_retrievers: tweak to optimize workload
            max_simultaneous_downloaders: tweak to optimize workload
            max_simultaneous_part_handlers: tweak to optimize workload
        """
        # """Implements abstract constructor."""
        super().__init__(drs_client=drs_client)
        self.max_simultaneous_object_retrievers = max_simultaneous_object_retrievers
        self.max_simultaneous_object_signers = max_simultaneous_object_signers
        self.max_simultaneous_downloaders = max_simultaneous_downloaders
        self.max_simultaneous_part_handlers = max_simultaneous_part_handlers
        self.disable = not show_progress
        self.part_size = part_size

    @staticmethod
    def _parts_generator(size: int, start: int = 0, part_size: int = None) -> Iterator[Tuple[int, int]]:
        """Determine the start,size for each part

        Args:
            size: size of file
            start: offset into file 0 based
            part_size: desired part size

        Returns:
            iterator of tuple start, size
        """
        while size - start > part_size:
            yield start, start + part_size
            start += part_size + 1
            # start += part_size
        yield start, size

    async def _run_download_parts(self, drs_object: DrsObject, destination_path: Path) -> DrsObject:
        """Determine number of parts for signed url and create tasks for each part, run them in batches.

        Args:
            drs_object: Information about a bucket object

        Returns:
            list of paths to files for each part, in order.
        """
        # create a list of parts
        parts = []
        for start, size in self._parts_generator(size=drs_object.size, part_size=self.part_size):
            parts.append((start, size, ))

        if len(parts) > 1000:
            logger.error(f'tasks > 1000 {drs_object.name} has over 1000 parts, consider optimization. ({len(parts)})')

        paths = []
        # TODO - tqdm ugly here?
        for chunk_parts in \
                tqdm.tqdm(DrsAsyncManager.chunker(parts, self.max_simultaneous_part_handlers),
                          total=math.ceil(len(parts) / self.max_simultaneous_part_handlers),
                          desc="File Download Progress",
                          leave=False,
                          disable=self.disable):
            chunk_tasks = []
            for start, size in chunk_parts:
                task = asyncio.create_task(self._drs_client.download_part(drs_object=drs_object, start=start, size=size,
                                                                          destination_path=destination_path))
                chunk_tasks.append(task)

            chunk_paths = [
                await f
                for f in
                tqdm.tqdm(asyncio.as_completed(chunk_tasks), total=len(chunk_tasks), leave=False,
                          desc=f"    * {drs_object.name} Part", disable=self.disable)
            ]
            # something bad happened
            if None in chunk_paths:
                logger.error(f"{drs_object.name} had missing part.")
                return drs_object

            paths.extend(chunk_paths)

        drs_object.file_parts = paths

        i = 1
        filename = f"{drs_object.name}"
        original_file_name = Path(filename)
        while True:
            if os.path.isfile(destination_path.joinpath(filename)):
                filename = f"{original_file_name}({i})"
                i = i + 1
                continue
            break

        # re-assemble and test the file parts
        # hash function dynamic
        checksum_type = drs_object.checksums[0].type
        assert checksum_type in hashlib.algorithms_available, f"Checksum {checksum_type} not supported."
        checksum = hashlib.new(checksum_type)
        with open(destination_path.joinpath(filename), 'wb') as wfd:
            # sort the items of the list in place - Numerically based on start i.e. "xxxxxx.start.end.part"
            drs_object.file_parts.sort(key=lambda x: int(str(x).split('.')[-3]))
            for f in drs_object.file_parts:
                fd = open(f, 'rb')
                wrapped_fd = Wrapped(fd, checksum)
                # efficient way to write
                shutil.copyfileobj(wrapped_fd, wfd)
                # explicitly close all
                wrapped_fd.close()
                fd.close()
                wfd.flush()
        actual_checksum = checksum.hexdigest()

        # compare calculated md5 vs expected
        expected_checksum = drs_object.checksums[0].checksum
        if expected_checksum != actual_checksum:
            msg = f"Actual {checksum_type} hash {actual_checksum} does not match expected {expected_checksum}"
            drs_object.errors.append(msg)
            # if error leave parts in place for now
        else:
            # ok, so clean up file parts
            for f in drs_object.file_parts:
                f.unlink()

        return drs_object

    async def _run_download(self, drs_objects: List[DrsObject], destination_path: Path) -> List[DrsObject]:
        """
        Create tasks to sign and download, display progress.
        Args:
            drs_objects: list of drs objects to download

        Returns:
            updated list of drs objects
        """

        # first sign the urls
        tasks = []
        for drs_object in drs_objects:
            task = asyncio.create_task(self._drs_client.sign_url(drs_object=drs_object))
            tasks.append(task)

        drs_objects_with_signed_urls = [
            await f for f in asyncio.as_completed(tasks)
        ]

        # second, download the parts
        tasks = []
        for drs_object in drs_objects_with_signed_urls:
            task = asyncio.create_task(
                self._run_download_parts(drs_object=drs_object, destination_path=destination_path))
            tasks.append(task)

        drs_objects_with_file_parts = [
            await f
            for f in asyncio.as_completed(tasks)
        ]

        return drs_objects_with_file_parts

    async def _run_get_objects(self, object_ids: List[str], leave: bool) -> List[DrsObject]:
        """Create async tasks to retrieve list DrsObject, displays progress.

        Args:
            object_ids: object_id from manifest
            leave: leave flag to keep progress indicator displayed

        Returns:

        """

        tasks = []
        for object_id in object_ids:
            task = asyncio.create_task(self._drs_client.get_object(object_id=object_id))
            tasks.append(task)

        signed_urls = [
            await f
            for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks), leave=leave,
                               desc="retrieving object information", disable=self.disable)
        ]

        return signed_urls

    @classmethod
    def chunker(cls, seq: Collection, size: int) -> Iterator:
        """Iterate over a list in chunks.

        Args:
            seq: an iterable
            size: desired chunk size

        Returns:
            an iterator that returns lists of size or less
        """
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def get_objects(self, object_ids: List[str]) -> List[DrsObject]:
        """Create tasks for all object_ids, run them in batches, get information about the object.

        Args:
            object_ids: list of objects to fetch
        """

        drs_objects = []

        total_batches = len(object_ids) / self.max_simultaneous_object_retrievers
        # rounding
        # this would imply that if batch count is 9.3, and you round down the last .3 is never
        # actually downloaded since there are only 9 batches. math.ciel would round up if there is a decimal at all
        if math.ceil(total_batches) - total_batches > 0:
            total_batches += 1
            total_batches = int(total_batches)

        current = 0

        for chunk_of_object_ids in DrsAsyncManager.chunker(object_ids, self.max_simultaneous_object_retrievers):

            drs_objects.extend(
                asyncio.run(self._run_get_objects(object_ids=chunk_of_object_ids, leave=(current == total_batches))))
            current += 1

        return drs_objects

    def download(self, drs_objects: List[DrsObject], destination_path: Path) -> List[DrsObject]:
        """Split the drs_objects into manageable sizes, download the files.

        Args:
            drs_objects: list of DrsObject
            destination_path: directory where to write files when complete

        Returns:
            DrsObjects updated with _file_parts

        """
        total_batches = len(drs_objects) / self.max_simultaneous_downloaders
        # if fractional, add 1
        if math.ceil(total_batches) - total_batches > 0:
            total_batches += 1

        current = 0
        updated_drs_objects = []

        for chunk_of_drs_objects in DrsAsyncManager.chunker(drs_objects, self.max_simultaneous_object_retrievers):
            completed_chunk = asyncio.run(self._run_download(drs_objects=chunk_of_drs_objects,
                                                             destination_path=destination_path))

            updated_drs_objects.extend(completed_chunk)
            current += 1

        return updated_drs_objects

    def optimize_workload(self, drs_objects: List[DrsObject]) -> List[DrsObject]:
        """
        Optimize the workload, sort prioritize and set thread management parameters.
        Args:
            drs_objects:

        Returns:
            same list that was passed
        """
        # Now that we have the objects to download, we have an opportunity to shape the downloads
        # e.g. are the smallest files first?  tweak MAX_* to optimize per workload

        if len(drs_objects) == 1:
            self.max_simultaneous_part_handlers = 50
            self.part_size = 64 * MB
            self.max_simultaneous_downloaders = 10

        elif any(drs_object.size > (1 * GB) for drs_object in drs_objects):
            self.max_simultaneous_part_handlers = 10
            self.part_size = 10 * MB
            self.max_simultaneous_downloaders = 10

        elif all((drs_object.size < (5 * MB)) for drs_object in drs_objects):
            self.part_size = 5 * MB
            self.max_simultaneous_part_handlers = 1
            self.max_simultaneous_downloaders = 10

        else:
            self.part_size = 10 * MB
            self.max_simultaneous_part_handlers = 10
            self.max_simultaneous_downloaders = 10

        return drs_objects
