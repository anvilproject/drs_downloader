"""Main module for the DRS downloader package.

Configuration options allow for optimizing the download process:

- Object retrievers: The number of DRS objects are retrieved in a given batch.
- Object signers: The number of DRS objects signed in a given batch.
- Downloaders: The number of simultaneous downloads to start in a given batch.
- Part handlers: The number of parts to download at a given time.
- Part size: size in bytes for each downloadable part of a given DRS object.
"""

KB = 1024
MB = KB * KB
GB = MB * KB

DEFAULT_MAX_SIMULTANEOUS_OBJECT_RETRIEVERS = 100
DEFAULT_MAX_SIMULTANEOUS_OBJECT_SIGNERS = 10
DEFAULT_MAX_SIMULTANEOUS_DOWNLOADERS = 10
DEFAULT_MAX_SIMULTANEOUS_PART_HANDLERS = 3
DEFAULT_PART_SIZE = 10 * MB
