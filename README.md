# DRS Downloader <!-- omit from toc -->

[![DRS Downloader][build-badge]][build-link]

[build-badge]: https://github.com/anvilproject/drs_downloader/actions/workflows/build.yml/badge.svg
[build-link]: https://github.com/anvilproject/drs_downloader/actions/workflows/build.yml

A file download tool for AnVIL/TDR data identified by DRS URIs and Google

- [Installation](#installation)
- [Usage](#usage)
  - [Quick Start](#quick-start)
  - [Example](#example)
  - [Additional Options](#additional-options)
- [Development](#development)
  - [Tests](#tests)
- [Authentication](#authentication)
- [Credits](#credits)
- [Contributing](#contributing)
- [Project layout](#project-layout)

## Installation

Download the latest `drs_downloader` zip file for your operating system from the [releases](https://github.com/anvilproject/drs_downloader/releases/latest) page. Unzip the downloaded file and run the `drs_downloader` executable from the command line.

## Usage

### Quick Start

```sh
drs_downloader terra -m <manifest file> -d <destination directory>
```

### Example

The below command is a basic example of how to structure a download command with all of the required arguments:

```sh
$ drs_downloader terra -m tests/fixtures/terra-data.tsv -d DATA
100%|████████████████████████████████| 10/10 [00:00<00:00, 56148.65it/s]

2022-11-21 16:56:49,595 ('HG03873.final.cram.crai', 'OK', 1351946, 1)
2022-11-21 16:56:49,595 ('HG04209.final.cram.crai', 'OK', 1338980, 1)
2022-11-21 16:56:49,595 ('HG02142.final.cram.crai', 'OK', 1405543, 1)
2022-11-21 16:56:49,595 ('HG01552.final.cram.crai', 'OK', 1296198, 1)
2022-11-21 16:56:49,595 ('NA18613.final.cram.crai', 'OK', 1370106, 1)
2022-11-21 16:56:49,595 ('HG00536.final.cram.crai', 'OK', 1244278, 1)
2022-11-21 16:56:49,595 ('HG02450.final.cram.crai', 'OK', 1405458, 1)
2022-11-21 16:56:49,595 ('NA20525.final.cram.crai', 'OK', 1337382, 1)
2022-11-21 16:56:49,595 ('NA20356.final.cram.crai', 'OK', 1368064, 1)
2022-11-21 16:56:49,595 ('HG00622.final.cram.crai', 'OK', 1254920, 1)
2022-11-21 16:56:49,595 ('done', 'statistics.max_files_open', 37) 

$ ls ./DATA
HG00536.final.cram.crai HG01552.final.cram.crai
HG02450.final.cram.crai HG04209.final.cram.crai
NA20356.final.cram.crai HG00622.final.cram.crai
HG02142.final.cram.crai HG03873.final.cram.crai
NA18613.final.cram.crai NA20525.final.cram.crai
```

### Additional Options

To see all available flags run the `help` command:

```sh
drs_downloader terra --help
```

```sh
Usage: drs_download terra [OPTIONS]

  Copy files from terra.bio

Options:
  -s, --silent                Display nothing.
  -d, --destination_dir TEXT  Destination directory.  [default: /tmp/testing]
  -m, --manifest_path TEXT    Path to manifest tsv.  [default:
                              tests/fixtures/terra-data.tsv]
  --help                      Show this message and exit.
```
## Development

To get ready for development first get the code:

```sh
git clone https://github.com/anvilproject/drs_downloader
cd drs_downloader
```

Then create and activate a virtual environment using `Python3.9`:

```sh
python3.9 -m venv venv
. venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

Now you should be ready to start coding and testing!

### Tests

All tests and test files are stored in the `tests` directory. Pytest is used as the testing framework. To run all tests with a coverage report run `pytest` with the `--cov=tests` flag:

```sh
$ pytest --cov=tests

========================= test session starts =========================
platform darwin -- Python 3.9.4, pytest-7.2.0, pluggy-1.0.0
rootdir: /Users/beckmanl/code/drs_downloader, configfile: pyproject.toml
plugins: cov-4.0.0, anyio-3.6.2
collected 4 items

tests/unit/test_main.py ...                                       [ 75%]
tests/unit/test_basic_cli.py .                                    [100%]

---------- coverage: platform darwin, python 3.9.4-final-0 -----------
Name                           Stmts   Miss  Cover
--------------------------------------------------
tests/unit/test_main.py                41      0   100%
tests/unit/test_basic_cli.py       3      0   100%
--------------------------------------------------
TOTAL                             44      0   100%


========================== 4 passed in 14.68s ==========================
```

## Authentication

In order to get the downloader to work, you will need to install Google gcloud CLI on your local machine. https://cloud.google.com/sdk/docs/install

Next, you must connect the google account that your Terra account connected to gcloud. This is done with gcloud auth login:

```sh
gcloud auth login
```

You need to have a terra project that is set up for billing. Once you get one, go to your terra workspaces page: https://anvil.terra.bio/#workspaces/

Click on the project that you want to bill to. On the righthand corner of the screen click on Cloud Information heading.

Copy and paste the Google Project Id field into the below command:

```sh
gcloud config set project <project ID>
```

Next, you need to link your Google account to the location where the DRS URIs will download from. This is endpoint specific.

Go to this page: https://anvil.terra.bio/#profile?tab=externalIdentities

If you are logging into bio data catalyst do the following:
1. Right click on the log in/renew  button.
2. Select copy url.
3. Copy this link in another tab but instead of pressing enter go to the end of the URL that was copied
and change the suffix of the URL from =[old suffix] to =google

If your URIs are not  from bio data catalyst then authenticate with your Terra Linked Google account on the other
sites.

Now run `gcloud auth print-access-token`. This should return a long string of letters an numbers. If it doesn't then
your Terra google account is probably not linked with your gcloud account.

To test that this setup returns signed URLs copy and paste the below curl command into your terminal, but instead of running it replace [URI] with a DRS uri that belongs to a small file from your TSV file. By running this in terminal you should get back a signed URL that you can copy and paste into your browser to download a file.

```sh
curl --request POST  --url https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v3  --header "authorization: Bearer $(gcloud auth print-access-token)"  --header 'content-type: application/json'  --data '{ "url": "[URI]", "fields": ["fileName", "size", "hashes", "accessUrl"] }'
```

If you can run the above command with your own drs URI than you are setup to run the command line tool.


## Credits

This project is developed in partnership between The AnVIL Project, the Broad Institute, and the Ellrott Lab at Oregon Health & Science University. Development is lead by Brian Walsh with contributions from Matthew Peterkort and Liam Beckman. Special thanks to Michael Baumann at the Broad Institute for guidance and development recommendations.

## Contributing

Pull requests, issues, and feature requests welcome. See the Development section how to set up the development environment.


## Project layout

```sh
┌── LICENSE
├── README.md
├── docs
│   ├── index.md         # The documentation homepage
│   └── mkdocs.yml
├── drs_downloader       # Source directory
│   ├── clients
│   ├── manager.py
│   └── models.py
├── requirements-dev.txt # Installation dependencies
├── requirements.txt     # Development dependencies
├── setup.py             # Setuptools file, used by Pyinstaller and pip
└── tests
    ├── fixtures         # Test manifest files
    └── unit             # Unit tests
 ```
 