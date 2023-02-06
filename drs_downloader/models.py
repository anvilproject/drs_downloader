import os
import platform
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional


@dataclass
class AccessMethod(object):
    """See https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/#_accessmethod"""

    access_url: str
    """An AccessURL that can be used to fetch the actual object bytes."""
    type: str
    """Type of the access method. enum (s3, gs, ftp, gsiftp, globus, htsget, https, file)"""


@dataclass
class AccessURL(object):
    """See https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/#_accessurl"""

    headers: Dict[str, str]
    """An optional list of headers to include in the HTTP request to url."""
    url: str
    """A fully resolvable URL that can be used to fetch the actual object bytes."""


@dataclass
class Checksum(object):
    """See https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/#_checksum"""

    checksum: str
    """The hex-string encoded checksum for the data."""
    type: str
    """The digest method used to create the checksum."""


@dataclass
class DrsObject(object):
    """See https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/#_drsobject"""

    id: str
    """An identifier unique to this DrsObject."""
    self_uri: str
    """A drs:// URI, as defined in the DRS documentation, that tells clients how to access this object."""
    checksums: List[Checksum]
    """Needed for integrity check."""
    size: int
    """Needed for multi part download."""
    name: str
    """A string that can be used to name a DrsObject."""
    file_parts: List[Path] = field(default_factory=list)
    """List of file parts in order of assembly."""
    errors: List[str] = field(default_factory=list)
    """List of errors."""
    access_methods: List[AccessMethod] = field(default_factory=list)
    """Signed url."""


@dataclass
class Statistics:
    """This is where we can share data between threads"""

    lock: threading.Lock = threading.Lock()
    max_files_open: int = 0
    pid: object = os.getpid()

    def set_max_files_open(self):
        """Threadsafe way to determine current number of open files.

        Note: only tested on Mac
        """
        self.lock.acquire()
        system = platform.system()
        if system == "Darwin":
            open_fd = len(set(os.listdir("/dev/fd/")))
        elif system == "Windows":
            # TODO install psutils - len(Process.open_files())
            open_fd = 0
        else:
            open_fd = len(set(os.listdir(f"/proc/{self.pid}/fd/")))
        if open_fd > self.max_files_open:
            self.max_files_open = open_fd
        self.lock.release()


class DrsClient(ABC):
    """Interact with DRS service."""

    def __init__(self, statistics: Statistics = Statistics()):
        self.statistics = statistics

    @abstractmethod
    async def download_part(
        self, drs_object: DrsObject, start: int, size: int, destination_path: Path, verbose: bool = False
    ) -> Optional[Path]:

        """Download and save part of a file to disk; on error, update drs_object.errors return None

        Args:
            destination_path: where to save the part
            drs_object: state of download
            start: segment start
            size: segment end
        """
        pass

    @abstractmethod
    async def sign_url(self, drs_object: DrsObject) -> DrsObject:
        """Retrieve signed url from service return populated DrsObject AccessMethod

        Args:
            drs_object:

        Returns:
            populated DrsObject
        """
        pass

    @abstractmethod
    async def get_object(self, object_id: str) -> DrsObject:
        """Retrieve size, checksums, etc. populate DrsObject."""
        pass
