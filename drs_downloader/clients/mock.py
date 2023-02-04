import asyncio
import hashlib
import logging
import os
import tempfile
import uuid
from pathlib import Path
import random
from typing import Optional

from drs_downloader import MB
from drs_downloader.models import DrsClient, DrsObject, AccessMethod, Checksum

logger = logging.getLogger(__name__)

MAX_SIZE_OF_OBJECT = 50 * MB

# special identifiers that will prompt failures
INCORRECT_SIZE = "drs://" + str(uuid.uuid5(uuid.NAMESPACE_DNS, "INCORRECT_SIZE"))
BAD_ID = "drs://" + str(uuid.uuid5(uuid.NAMESPACE_DNS, "BAD_ID"))
BAD_MD5 = "drs://" + str(uuid.uuid5(uuid.NAMESPACE_DNS, "BAD_MD5"))
BAD_SIGNATURE = "drs://" + str(uuid.uuid5(uuid.NAMESPACE_DNS, "BAD_SIGNATURE"))


class MockDrsClient(DrsClient):
    """Simulate responses from server."""

    async def sign_url(self, drs_object: DrsObject, verbose: bool = False) -> Optional[DrsObject]:
        """Simulate url signing by waiting 1-3 seconds, return populated DrsObject

        Args:
            drs_object:

        Returns:
            populated DrsObject
        """
        # simulate a failed signature
        if drs_object.id == BAD_SIGNATURE:
            return None

        # here we sleep while the file is open and measure total files open
        fp = tempfile.TemporaryFile()
        sleep_duration = random.randint(1, 3)
        await asyncio.sleep(delay=sleep_duration)
        fp.write(b"sign url")
        self.statistics.set_max_files_open()
        fp.close()

        # provide expected result, e.g. X-Signature
        access_url = f"{drs_object.self_uri}?X-Signature={uuid.uuid1()}"
        # place it in the right spot in the drs object
        drs_object.access_methods.append(AccessMethod(access_url=access_url, type="gs"))

        return drs_object

    async def download_part(
        self, drs_object: DrsObject, start: int, size: int, destination_path: Path, verbose: bool = False
    ) -> Path:
        """Actually download a part.

        Args:
            destination_path:
            drs_object:
            start:
            size:

        Returns:
            full path to that part.
        """

        # calculate actual part size from range see https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range

        length_ = size - start + 1
        # logger.info((drs_object.name, start, length_))
        # logger.error("ERROR1 %s",drs_object)

        if BAD_ID in drs_object.self_uri:
            logger.warning(f"Mock bad id {drs_object.self_uri}")
            drs_object.errors = ["Mock error BAD_ID"]
            return None

        with open(Path(os.getcwd(), f"{drs_object.name}.golden"), "rb") as f:
            f.seek(start)
            data = f.read(length_)

        (fd, name,) = tempfile.mkstemp(
            prefix=f"{drs_object.name}.{start}.{size}.",
            suffix=".part",
            dir=str(destination_path),
        )
        with os.fdopen(fd, "wb") as fp:
            sleep_duration = random.randint(1, 3)
            await asyncio.sleep(delay=sleep_duration)
            fp.write(data)
            self.statistics.set_max_files_open()
            fp.close()

        return Path(name)

    async def get_object(self, object_id: str, verbose: bool = False) -> DrsObject:
        """Fetch the object from repository DRS Service.

        See https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/#_getobject.

        Args:
            object_id:

        Returns:

        """
        # """Actually fetch the object.
        #
        # """
        fp = tempfile.TemporaryFile()
        sleep_duration = random.randint(1, 3)
        await asyncio.sleep(delay=sleep_duration)
        fp.write(b"get object")
        self.statistics.set_max_files_open()
        fp.close()

        id_ = str(uuid.uuid4())
        name_ = f"file-{id_}.txt"

        line = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\n"  # noqa
        line_len = len(line)
        number_of_lines = int(random.randint(line_len, MAX_SIZE_OF_OBJECT) / line_len)
        lines = line * number_of_lines
        size_ = len(lines)

        # write it for testing
        destination_dir = Path(os.getcwd())
        destination_dir.mkdir(parents=True, exist_ok=True)
        with open(Path(f"{destination_dir}/{name_}.golden"), "wb") as f:
            f.write(lines)

        checksum = Checksum(hashlib.new("md5", lines).hexdigest(), type="md5")

        # simulate an incorrect MD5
        if object_id == BAD_MD5:
            checksum = Checksum(hashlib.new("md5", line).hexdigest(), type="md5")

        # simulate an incorrect size
        if object_id == INCORRECT_SIZE:
            size_ += 1000

        return DrsObject(
            self_uri=object_id,
            size=size_,
            # md5, etag, crc32c, trunc512, or sha1
            checksums=[checksum],
            id=id_,
            name=name_,
        )


# test helpers


def manifest_all_ok(number_of_object_ids):
    """Generate a test manifest, a tsv file with valid drs identifiers."""
    ids_from_manifest = [str(uuid.uuid4()) for _ in range(number_of_object_ids)]
    tsv_file = tempfile.NamedTemporaryFile(delete=False, mode="w")
    tsv_file.write("ga4gh_drs_uri\n")
    for id_ in ids_from_manifest:
        tsv_file.write(f"drs://{id_}\n")
    tsv_file.close()
    return tsv_file


def manifest_bad_file_size():
    """Generate a test manifest, a tsv file with 2 valid drs identifiers and one that will create an incorrect file."""
    ids_from_manifest = [
        "drs://" + str(uuid.uuid4()),
        INCORRECT_SIZE,
        "drs://" + str(uuid.uuid4()),
    ]
    tsv_file = tempfile.NamedTemporaryFile(delete=False, mode="w")
    tsv_file.write("ga4gh_drs_uri\n")
    for id_ in ids_from_manifest:
        tsv_file.write(f"{id_}\n")
    tsv_file.close()
    return tsv_file


def manifest_bad_id_for_download():
    """Generate a test manifest, a tsv file with 2 valid drs identifiers and one that will create an incorrect file."""
    ids_from_manifest = [
        "drs://" + str(uuid.uuid4()),
        "drs://" + str(uuid.uuid4()),
        BAD_ID,
        "drs://" + str(uuid.uuid4()),
    ]
    tsv_file = tempfile.NamedTemporaryFile(delete=False, mode="w")
    tsv_file.write("ga4gh_drs_uri\n")
    for id_ in ids_from_manifest:
        tsv_file.write(f"{id_}\n")
    tsv_file.close()
    return tsv_file
