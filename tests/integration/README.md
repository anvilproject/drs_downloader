# Integration tests

Tests in this folder require a drs server with a known population of data objects


## TODO - tests to be written

* failed download - a test that expects the download to start and then fail
* parse without header - a test with a manifest missing the expected header column
* bad credentials file - a gen3 test with a bad credential key
* authorization failed - a gen3 test good credential key, but not authorized to download 
* test with missing google auth - a terra test without google auth
* Part download failed - a terra test with a bad part (bad checksum)
* Unauthorized google auth - a terra test with a valid google credential, but not authorized to download
* A test with Parts > 1000  - test a very big file, the system will prompt with a warning
* A test that overwrites an existing file
* workflow mgt - manifest with 1 file
* workflow mgt - any file in manifest > 1 GB


# Needed

* DRS URI that doesn't return name of file to test URL splitting. manager.py:184

* Write test for checking no destination path specified downloads to current working directory. https://github.com/anvilproject/drs_downloader/pull/15#discussion_r1051637307

* Write test for checking part size == 128MB on batches with 1GB files. https://github.com/anvilproject/drs_downloader/pull/15#discussion_r1051653484