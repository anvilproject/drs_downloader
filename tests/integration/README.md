# Integration tests

Tests in this folder require a drs server with a known population of data objects


## TODO - tests to be written

* failed download - a test that expects the download to start and then fail
* Part download failed - a terra test with a bad part (bad checksum)

#Add extra flag notation for 

TERRA DATA REPOSITORY 
* Unauthorized google auth - a terra test with a valid google credential, but not authorized to download



* A test that overwrites an existing file


DONE
* parse without header - a test with a manifest missing the expected header column
* bad credentials file - a gen3 test with a bad credential key
* test with missing google auth - a terra test without google auth
* Partial download recovery 
* workflow mgt - manifest with 1 file 
* workflow mgt - any file in manifest > 1 GB 
* URIS NOT FOUND GEN3 AND TERRA
* A test with Parts > 1000  - test a very big file, the system will prompt with a warning
* authorization failed - a gen3 test good credential key, but not authorized to download 



