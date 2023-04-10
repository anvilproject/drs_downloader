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
import sys
import time

# import time

from drs_downloader import (
    DEFAULT_MAX_SIMULTANEOUS_OBJECT_RETRIEVERS,
    DEFAULT_MAX_SIMULTANEOUS_PART_HANDLERS,
    DEFAULT_MAX_SIMULTANEOUS_DOWNLOADERS,
    DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS,
    DEFAULT_PART_SIZE,
    MB,
    GB,
)

from drs_downloader.models import DrsClient, DrsObject

logger = logging.getLogger()

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)

file_handler = logging.FileHandler("drs_downloader.log")
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S %Z")
# logging.Formatter.converter = gmtime
file_handler.setFormatter(formatter)
stdout_handler.setFormatter(formatter)

logging.getLogger().setLevel(logging.INFO)
logger.addHandler(stdout_handler)
logger.addHandler(file_handler)


class DrsManager(ABC):
    """Manage DRSClient workload."""

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
    def download(
        self, drs_objects: List[DrsObject], destination_path: Path
    ) -> List[DrsObject]:
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
    """Manage DRSClient workload with asyncio threads, display progress."""

    def __init__(
        self,
        drs_client: DrsClient,
        show_progress: bool = True,
        part_size: int = DEFAULT_PART_SIZE,
        max_simultaneous_object_retrievers=DEFAULT_MAX_SIMULTANEOUS_OBJECT_RETRIEVERS,
        max_simultaneous_downloaders=DEFAULT_MAX_SIMULTANEOUS_DOWNLOADERS,
        max_simultaneous_part_handlers=DEFAULT_MAX_SIMULTANEOUS_PART_HANDLERS,
        max_simultaneous_object_signers=DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS,
    ):
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
    def _parts_generator(
        size: int, start: int = 0, part_size: int = None
    ) -> Iterator[Tuple[int, int]]:
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

    async def wait_till_completed(self, tasks, err_function_msg):
        drs_objects_with_signed_urls = []
        while tasks:
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for t in tqdm.tqdm(done, total=len(tasks),
                               desc=f"retrieving {err_function_msg} information",
                               disable=self.disable):
                try:
                    y = await t
                    drs_objects_with_signed_urls.append(y)

                except Exception:
                    signed_url = DrsObject(
                                    self_uri="",
                                    id="",
                                    checksums=[],
                                    size=0,
                                    name=None,
                                    errors=[f"Exception in {err_function_msg} function"],
                                )

                    drs_objects_with_signed_urls.append(signed_url)
                    t.cancel()

        return drs_objects_with_signed_urls

    async def _run_download_parts(
        self, drs_object: DrsObject, destination_path: Path, verbose: bool
    ) -> DrsObject:
        """Determine number of parts for signed url and create tasks for each part, run them in batches.

        Args:
            drs_object: Information about a bucket object

        Returns:
            list of paths to files for each part, in order.
        """
        # create a list of parts
        parts = []
        for start, size in self._parts_generator(
            size=drs_object.size, part_size=self.part_size
        ):
            parts.append(
                (
                    start,
                    size,
                )
            )

        if len(parts) > 1000:
            if verbose:
                logger.warning(f'Warning: tasks > 1000 {drs_object.name} has over 1000 parts and is a large download. \
                ({len(parts)})')

        if drs_object.size > 20 * MB:
            self.disable = False
        else:
            self.disable = True

        paths = []
        # TODO - tqdm ugly here?
        for chunk_parts in tqdm.tqdm(
            DrsAsyncManager.chunker(parts, self.max_simultaneous_part_handlers),
            total=math.ceil(len(parts) / self.max_simultaneous_part_handlers),
            desc="File Download Progress",
            file=sys.stdout,
            leave=False,
            disable=self.disable,
        ):
            chunk_tasks = []
            existing_chunks = []
            for start, size in chunk_parts:
                # Check if part file exists and if so verify the expected size.
                # If size matches the expected value then return the Path of the file_name for eventual reassembly.
                # If size does not match then attempt to restart the download.
                file_name = destination_path / f"{drs_object.name}.{start}.{size}.part"
                file_path = Path(file_name)

                if self.check_existing_parts(file_path, start, size, verbose):
                    existing_chunks.append(file_path)
                    continue

                task = asyncio.create_task(
                    self._drs_client.download_part(
                        drs_object=drs_object,
                        start=start,
                        size=size,
                        destination_path=destination_path,
                        verbose=verbose
                    )
                )
                chunk_tasks.append(task)

            chunk_paths = await self.wait_till_completed(chunk_tasks, "download_parts")

            """
            Uncessesary logging message for the end user. When you take into account
            that most downloads are going to take longer than 15 minutes and this message
             will be spammed for every part that is already downlaoded when the resigning step happens.

            if len(existing_chunks) > 0:
                #logger.info(f"{drs_object.name} had {len(existing_chunks)} existing parts.")
            """

            chunk_paths.extend(existing_chunks)
            # something bad happened
            if None in chunk_paths:
                if any(
                    [
                        "RECOVERABLE in AIOHTTP" in str(error)
                        for error in drs_object.errors
                    ]
                ):
                    return drs_object
                else:
                    if verbose:
                        logger.error(f"{drs_object.name} had missing part.")
                    return drs_object

            paths.extend(chunk_paths)

        """
       print(" LIST OF DRS OBJECT ERRORS AFTER DLOAD IN RUN DOWNLOAD PARTS",drs_object.errors)
       print("RECOVERABLE? ",any(['RECOVERABLE' in str(error) for error in drs_object.errors]))

        if(any(['RECOVERABLE' in str(error) for error in drs_object.errors])):
                return drs_object
        """

        if (
            None not in chunk_paths
            and len(existing_chunks) == 0
            and self.disable is True
        ):
            if verbose:
                logger.info("%s Downloaded sucessfully", drs_object.name)

        drs_object.file_parts = paths

        i = 1
        filename = (
            f"{drs_object.name}"
            or drs_object.access_methods[0].access_url.split("/")[-1].split("?")[0]
        )
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
        assert (
            checksum_type in hashlib.algorithms_available
        ), f"Checksum {checksum_type} not supported."
        checksum = hashlib.new(checksum_type)
        with open(destination_path.joinpath(filename), "wb") as wfd:
            # sort the items of the list in place - Numerically based on start i.e. "xxxxxx.start.end.part"
            drs_object.file_parts.sort(key=lambda x: int(str(x).split(".")[-3]))

            T_0 = time.time()
            for f in tqdm.tqdm(
                drs_object.file_parts,
                total=len(drs_object.file_parts),
                desc=f"       {drs_object.name:50.50} stitching",
                file=sys.stdout,
                leave=False,
                disable=self.disable,
            ):
                fd = open(f, "rb")  # NOT ASYNC
                wrapped_fd = Wrapped(fd, checksum)
                # efficient way to write
                await asyncio.to_thread(
                    shutil.copyfileobj, wrapped_fd, wfd, 1024 * 1024 * 10
                )
                # explicitly close all
                wrapped_fd.close()
                f.unlink()
                fd.close()
                wfd.flush()
            T_FIN = time.time()
            if verbose:
                logger.info(f"TOTAL 'STITCHING' (md5 10*MB no flush) TIME {T_FIN-T_0} {original_file_name}")
        actual_checksum = checksum.hexdigest()

        actual_size = os.stat(Path(destination_path.joinpath(filename))).st_size

        # compare calculated md5 vs expected
        expected_checksum = drs_object.checksums[0].checksum
        if expected_checksum != actual_checksum:
            msg = f"Actual {checksum_type} hash {actual_checksum} does not match expected {expected_checksum}"
            if verbose:
                logger.error(f"Actual {checksum_type} hash {actual_checksum} \
                             does not match expected {expected_checksum}")
            drs_object.errors.append(msg)

        if drs_object.size != actual_size:
            msg = f"The actual size {actual_size} does not match expected size {drs_object.size}"
            drs_object.errors.append(msg)

        # parts will be purposefully saved if there is an error so that
        # recovery script can have a chance to rebuild the file

        return drs_object

    async def _run_download(
        self, drs_objects: List[DrsObject], destination_path: Path, verbose: bool
    ) -> List[DrsObject]:
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
            if len(drs_object.errors) == 0:
                task = asyncio.create_task(self._drs_client.sign_url(drs_object=drs_object, verbose=verbose))
                tasks.append(task)

        drs_objects_with_signed_urls = await self.wait_till_completed(tasks, "sign_url")

        tasks = []
        for drs_object in drs_objects_with_signed_urls:
            if len(drs_object.errors) == 0:
                task = asyncio.create_task(
                    self._run_download_parts(
                        drs_object=drs_object, destination_path=destination_path, verbose=verbose
                    )
                )
                tasks.append(task)

            else:
                logger.error(
                    f"{drs_object.id} has error {drs_object.errors}, not attempting anything further"
                )

        drs_objects_with_file_parts = await self.wait_till_completed(tasks, "run_download_parts")

        return drs_objects_with_file_parts

    async def _run_get_objects(
        self, object_ids: List[str], leave: bool, verbose: bool
    ) -> List[DrsObject]:
        """Create async tasks to retrieve list DrsObject, displays progress.

        Args:
            object_ids: object_id from manifest
            leave: leave flag to keep progress indicator displayed

        Returns:

        """

        tasks = []
        for object_id in object_ids:
            task = asyncio.create_task(self._drs_client.get_object(object_id=object_id, verbose=verbose))
            tasks.append(task)

        object_information = []

        object_information = await self.wait_till_completed(tasks, "get_object")

        return object_information

    @classmethod
    def chunker(cls, seq: Collection, size: int) -> Iterator:
        """Iterate over a list in chunks.

        Args:
            seq: an iterable
            size: desired chunk size

        Returns:
            an iterator that returns lists of size or less
        """
        return (seq[pos: pos + size] for pos in range(0, len(seq), size))

    def get_objects(self, object_ids: List[str], verbose: bool) -> List[DrsObject]:
        """Create tasks for all object_ids, run them in batches, get information about the object.

        Args:
            object_ids: list of objects to fetch
        """

        drs_objects = []

        total_batches = math.ceil(
            len(object_ids) / self.max_simultaneous_object_retrievers
        )
        # rounding
        # this would imply that if batch count is 9.3, and you round down the last .3 is never
        # actually downloaded since there are only 9 batches. math.ciel would round up if there is a decimal at all

        current = 0

        for chunk_of_object_ids in DrsAsyncManager.chunker(
            object_ids, self.max_simultaneous_object_retrievers
        ):

            drs_objects.extend(
                asyncio.run(
                    self._run_get_objects(
                        object_ids=chunk_of_object_ids, leave=(current == total_batches), verbose=verbose
                    )
                )
            )
            current += 1

        return drs_objects

    def download(
        self, drs_objects: List[DrsObject], destination_path: Path, duplicate: bool, verbose: bool
    ) -> List[DrsObject]:
        """Split the drs_objects into manageable sizes, download the files.

        Args:
            drs_objects: list of DrsObject
            destination_path: directory where to write files when complete

        Returns:
            DrsObjects updated with _file_parts
        """
        while True:

            filtered_objects = self.filter_existing_files(
                drs_objects, destination_path, duplicate=duplicate, verbose=verbose
            )
            if len(filtered_objects) < len(drs_objects):
                complete_objects = [
                    obj for obj in drs_objects if obj not in filtered_objects
                ]
                for obj in complete_objects:
                    if verbose:
                        logger.info(f"{obj.name} already exists in {destination_path}. Skipping download.")

                if len(filtered_objects) == 0:
                    logger.info(
                        f"All DRS objects already present in {destination_path}."
                    )
                    return

            current = 0
            updated_drs_objects = []

            for chunk_of_drs_objects in DrsAsyncManager.chunker(
                filtered_objects, self.max_simultaneous_object_retrievers
            ):

                completed_chunk = asyncio.run(
                    self._run_download(
                        drs_objects=chunk_of_drs_objects,
                        destination_path=destination_path, verbose=verbose
                    )
                )
                current += 1
                updated_drs_objects.extend(completed_chunk)
            if verbose:
                logger.info(f"UPDATED DRS OBJECTS \n\n {updated_drs_objects}")

            if "RECOVERABLE in AIOHTTP" not in str(updated_drs_objects):
                break

            else:
                if verbose:
                    logger.info("RECURSING \n\n\n")

            for drsobject in drs_objects:
                drsobject.errors.clear()

        return updated_drs_objects

    def optimize_workload(
        self, verbose, drs_objects: List[DrsObject]
    ) -> List[DrsObject]:
        """
        Optimize the workload, sort prioritize and set thread management parameters.
        Args:
            drs_objects:

        Returns:
            same list that was passed
        """
        # Now that we have the objects to download, we have an opportunity to shape the downloads
        # e.g. are the smallest files first?  tweak MAX_* to optimize per workload
        # TODO: If part sizes changed here, would this result in an error in test recovery?
        # Going to maek all part sizes 128MB to solve the problem above except for the small files because in that case
        # there is a pytest written for them that will fail otherwise

        if len(drs_objects) == 1:
            self.max_simultaneous_part_handlers = 50
            self.part_size = 64 * MB
            self.max_simultaneous_downloaders = 10
            if verbose:
                logger.info("part_size=%s", self.part_size)

        elif any(True for drs_object in drs_objects if (int(drs_object.size) > GB)):
            self.max_simultaneous_part_handlers = 3
            self.part_size = 128 * MB
            self.max_simultaneous_downloaders = 10
            if verbose:
                logger.info("part_size=%s", self.part_size)

        elif all((drs_object.size < (5 * MB)) for drs_object in drs_objects):
            self.part_size = 1 * MB
            self.max_simultaneous_part_handlers = 2
            self.max_simultaneous_downloaders = 10
            if verbose:
                logger.info("part_size=%s", self.part_size)
                logger.info("part_handlers=%s", self.max_simultaneous_part_handlers)

        else:
            self.part_size = 128 * MB
            self.max_simultaneous_part_handlers = 10
            self.max_simultaneous_downloaders = 10
            if verbose:
                logger.info("part_size=%s", self.part_size)
                logger.info("part_handlers=%s", self.max_simultaneous_part_handlers)

        return drs_objects

    def filter_existing_files(
        self, drs_objects: List[DrsObject], destination_path: Path, duplicate: bool, verbose: bool
    ) -> List[DrsObject]:
        """Remove any DRS objects from a given list if they are already exist in the destination directory.

        Args:
            drs_objects (List[DrsObject]): The DRS objects from the manifest (some may already be downloaded)
            destination_path (Path): Download destination that may contain partially downloaded files

        Returns:
            List[DrsObject]: The DRS objects that have yet to be downloaded
        """

        if verbose:
            logger.info(f"VALUE OF duplicate {duplicate}")
        if duplicate is True:
            return drs_objects

        # Testing File filtering by size also
        """
        for drs in drs_objects:
            logger.info(f"drs.size: {drs.size}  != os.path.getsize(drs.name) {os.path.getsize(drs.name)}")
            if(drs.size != os.path.getsize(drs.name)):
                logger.warning(f"{drs.name} is the wrong size,
                consider running this command again with the --duplicate flag
                 so that your current file with the same name is
                  not overwritten by this one that is on the path to downloading")
        """
        filtered_objects = [
            drs for drs in drs_objects if (drs.name not in os.listdir(destination_path))
            #  or drs.size != os.path.getsize(drs.name) <-- this is used for filtering out wrong sized stuff
        ]
        if verbose:
            logger.info(f"VALUE OF FILTERED OBJECTS {filtered_objects}")

        return filtered_objects

    def check_existing_parts(self, file_path: Path, start: int, size: int, verbose: bool) -> bool:
        """Checks if any file parts have already been downloaded. If a file part was partially downloaded then it
           prompts a new download process for that part.

        Args:
            file_path (Path): Path of the given file part (ex. HG00536.final.cram.crai.1048577.1244278.part)
            start (int): Beginning byte of the file part (ex. 1048577)
            size (int): Final byte of the file part (ex. 1244278)

        Returns:
            bool: True if the file part exists in the destination and has the expected file size, False otherwise
        """

        if file_path.exists():
            expected_size = size - start + 1
            if verbose:
                logger.info(f"EXPTECTED PART SIZE SIZE {expected_size}")

            actual_size = file_path.stat().st_size
            sizes_match = actual_size == expected_size
            if verbose:
                logger.info(f"ACTUAL SIZE {actual_size}")

            if sizes_match is True:
                # this logger message is really redundant when you are downloading large files.
                # For the purposes of cleaning up the UI on expired signed URLS going to comment this out for now
                # logger.info(f"{file_path.name} exists and has expected size. Skipping download.")
                return True

        return False
