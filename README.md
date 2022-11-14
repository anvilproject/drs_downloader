# DRS Downloader

[![DRS Downloader][build-badge]][build-link]

[build-badge]: https://github.com/anvilproject/drs_downloader/actions/workflows/build.yml/badge.svg?branch=minimal-viable
[build-link]: https://github.com/anvilproject/drs_downloader/actions/workflows/build.yml

## Description

Download tool for AnVIL/TDR data identified by DRS URIs and Google

## Table of Contents

- [Description](#description)
- [Table of Contents](#table-of-contents)
- [Installation](#installation)
  - [Development](#development)
    - [Authentication](#authentication)
- [Usage](#usage)
  - [Example](#example)
- [Credits](#credits)
- [Contributing](#contributing)
- [Tests](#tests)
- [Project layout](#project-layout)

## Installation

While developing:
  - create a virtual enironment with the two commands
  python -m venv venv
  . ./venv/bin/activate
  - navigate to the project home directory.
  - `pip install -r requirements.txt` will install the package and keep it updated without having to re-install.

### Development

#### Authentication

In order to get the downloader to work, you need to install Google gcloud CLI on your local machine.
https://cloud.google.com/sdk/docs/install

Next, you must connect the google account that your Terra account connected to to g cloud. This is done with gcloud auth login:

```sh
gcloud auth login
```
You need to have a terra project that is set up for billing. Once you get one, go to your terra workspaces page:
https://anvil.terra.bio/#workspaces/

Click on the project that you want to bill to. On the righthand corner of the screen click on Cloud Information heading.
Copy and paste the Google Project Id field into the below command:
```sh
gcloud config set project <project ID>
```
Next, you need to link your Google account to the location where the DRS URIs will download from. This is
endpoint specific.

Go to this page: https://anvil.terra.bio/#profile?tab=externalIdentities

If you are logging into bio data catalyst do the following:
1. Right click on the loggin/renew  button.
2. Select copy url.
3. Copy this link in another tab but instead of pressing enter go to the end of the URL that was copied
and change the suffix of the URL from =[old suffix] to =google

If your URIs are not  from bio data catalyst then authenticate with your Terra Linked Google account on the other
sites.

Now run gcloud auth print-access-token this should return a long string of letters an numbers. If it doesn't then
your Terra google account is probably not linked with your gcloud account.

to test that this setup returns signed URLs copy and paste the below curl command into your terminal, but
instead of running it replace [URI] with a DRS uri that belongs to a small file from your TSV file. By running this in terminal you should get back a signed URL that you can copy and paste into your browser to download a file.

curl --request POST  --url https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v3  --header "authorization: Bearer $(gcloud auth print-access-token)"  --header 'content-type: application/json'  --data '{ "url": "[URI]", "fields": ["fileName", "size", "hashes", "accessUrl"] }'

If you can run the above command with your own drs URI than you are setup to run the command line tool.

### Usage

```sh
drs_downloader <Input TSV File> <Destination>
```

The below command is a basic example of how to structure a download command with all of the required arguments
drs_downloader --tsvname ../tests/terra-data.tsv --destination DATA

This assumes that your TSV tests file is in the tests folder and that your destination folder is in the same folder as cli.py

Additionally there are also optional <--maxsigners> <--maxdownloaders> <--maxparts> flags
that are very useful for adjusting downloads of small or large files.

if you were downloading multiple large files and you wanted to see the progress in more parts you could run the commmand
drs_downloader --tsvname ../tests/terra-data.tsv --destination DATA --maxparts 20


### Example

```sh
$ drs_downloader tests/terra-data.tsv pfb:ga4gh_drs_uri /tmp/DATA
Welcome to the DRS Downloader!

Beginning download to /tmp/DATA
100%|████████████████████████████████████████████████████████████████████████████████| 10/10 [00:00<00:00, 56148.65it/s]
Downloading complete!

$ ls /tmp/DATA
HG00536.final.cram.crai HG01552.final.cram.crai HG02450.final.cram.crai HG04209.final.cram.crai NA20356.final.cram.crai
HG00622.final.cram.crai HG02142.final.cram.crai HG03873.final.cram.crai NA18613.final.cram.crai NA20525.final.cram.crai
```

## Credits

This project is developed in partnership between The AnVIL Project, the Broad Institute, and the Ellrott Lab at Oregon Health & Science University. Development is lead by Brian Walsh with contributions from Matthew Peterkort and Liam Beckman. Special thanks to Michael Baumann at the Broad Institute for guidance and development recommendations.

## Contributing

Pull requests, issues, and feature requests welcome.

## Tests

All tests and test files are stored in the `tests` directory. Pytest is used as the testing framework. To run all tests with a coverage report run:

```sh
pytest --cov=tests
```

```
============================= test session starts ==============================
platform darwin -- Python 3.9.4, pytest-7.2.0, pluggy-1.0.0
rootdir: /Users/beckmanl/code/drs_downloader
plugins: cov-4.0.0, anyio-3.6.2
collected 1 items

tests/test_main.py .                                                                                             [100%]

---------- coverage: platform darwin, python 3.9.4-final-0 -----------
Name                            Stmts   Miss  Cover
---------------------------------------------------
tests/test_main.py                  8      0   100%
---------------------------------------------------
TOTAL                              52     12    77%


============================== 1 passed in 17.43s ==============================

```

## Project layout

```sh
┌── LICENSE
├── README.md
├── docs
│   └── index.md            # The documentation homepage
├── drs_downloader          # Source directory
│   ├── __init__.py
│   ├── download.py         # Asynchronous file downloader, used by main.py and gen3_downloader.py
│   └── main.py             # Terra DRS downloader
├── mkdocs.yml              # MkDocs configuration file
├── requirements-dev.txt    # Required packages for development
├── requirements.txt        # Required packages for installation
├── Secrets
│   └── credentials.json    # API key for Gen3 acess
├── setup.py                # Setuptools file, used by Pyinstaller and pip
└── tests                   # All Python test and TSV files fo here
    ├── bdc_large_file_drs_uris_20221102.tsv
    ├── bdc_small_file_drs_uris_20221102.tsv
    ├── terra-data.tsv
    └── test_main.py
 ```
