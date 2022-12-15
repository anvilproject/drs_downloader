---------- coverage: platform darwin, python 3.9.12-final-0 ----------
Name                                 Stmts   Miss  Cover   Missing
------------------------------------------------------------------
drs_downloader/__init__.py               8      0   100%
drs_downloader/cli.py                  101      6    94%   114, 124-125, 127, 167, 183

MISSING TESTS:
114, 124-125,  127: add test for failed download
167: add test for parse without header

drs_downloader/clients/__init__.py       0      0   100%
drs_downloader/clients/gen3.py          92     14    85%   39-40, 42-47, 61, 68, 83-86

MISSING TESTS:
39-40: Bad credentials file
42-47: Authorization failed 
61: Authorization failed 
68: Will this statement ever be true?  Perhaps if auth token expires?

83-86: Part download failed

drs_downloader/clients/mock.py          55      0   100%
drs_downloader/clients/terra.py         85      9    89%   40, 72-75, 98-101

MISSING TESTS:
40: test with missing google auth
72-75: Part download failed
98-101: test with existing, but unauthorized google auth


drs_downloader/manager.py              166     20    88%   38, 64, 152, 176-177, 191, 214-215, 367-369, 372-374

MISSING TESTS:
152:  Parts > 1000 
176-177: Part download failed
191:  Rename file already downloaded
214-215: checksum failure
367-369: manifest with 1 file
372-374: any file in manifest > 1 GB


drs_downloader/models.py                61      6    90%   74-78, 102, 114, 119
------------------------------------------------------------------
TOTAL                                  568     55    90%