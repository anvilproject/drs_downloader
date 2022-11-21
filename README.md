# DRS Downloader

[![DRS Downloader][build-badge]][build-link]

[build-badge]: https://github.com/anvilproject/drs_downloader/actions/workflows/build.yml/badge.svg
[build-link]: https://github.com/anvilproject/drs_downloader/actions/workflows/build.yml

A file download tool for AnVIL/TDR data identified by DRS URIs and Google

- [Installation](#installation)
- [Usage](#usage)
  - [Quick Start](#quick-start)
  - [Example](#example)
  - [Large Files](#large-files)
  - [Additional Options](#additional-options)
- [Development](#development)
- [Authentication](#authentication)
- [Credits](#credits)
- [Contributing](#contributing)
- [Tests](#tests)
- [Project layout](#project-layout)

## Installation

Download the latest `drs_downloader` zip file for your operating system from the [releases](https://github.com/anvilproject/drs_downloader/releases/latest) page. Unzip the downloaded file and run the `drs_downloader` executable from the command line.

## Usage

### Quick Start

```sh
drs_downloader --tsv <input TSV file> --dest <download destination>
```

### Example

The below command is a basic example of how to structure a download command with all of the required arguments:

```sh
$ drs_downloader --tsv tests/terra-data.tsv --dest ./DATA
Welcome to the DRS Downloader!

Beginning download to DATA
100%|████████████████████████████████| 10/10 [00:00<00:00, 56148.65it/s]
Downloading complete!

$ ls ./DATA
HG00536.final.cram.crai HG01552.final.cram.crai
HG02450.final.cram.crai HG04209.final.cram.crai
NA20356.final.cram.crai HG00622.final.cram.crai
HG02142.final.cram.crai HG03873.final.cram.crai
NA18613.final.cram.crai NA20525.final.cram.crai
```

This assumes that your TSV tests file is in the `tests` folder and that your destination folder is `DATA`.

Additionally there are also optional `--maxsigners` `--maxdownloaders` `--maxparts` flags that are very useful for adjusting downloads of small or large files.

### Large Files

If you are downloading multiple large files and you want to see the progress in more parts you could run the command:

```sh
drs_downloader --tsv tests/terra-data.tsv --dest ./DATA --parts 20
```

### Additional Options

To see all available flags run the `help` command:

```sh
drs_downloader --help
```

```sh
Usage: main.py [OPTIONS]

Options:
  --tsv TEXT             The input TSV file. Example: terra-data.tsv
  --header TEXT          The column header in the TSV file associated with the
                         DRS URIs. Example: pfb:ga4gh_drs_uri
  --dest TEXT            The file path of the output file to download to.
                         Relative or Absolute. Example: /tmp/DATA
  --signers INTEGER      The maximum number of files to be signed at a time.
                         If you are downloading files in the GB this number
                         should be the same as downloaders flag. If this
                         variable is different than downloaders, you will run
                         into errors with files that take longer to download
                         than 15 minutes  [default: 10]
  --downloaders INTEGER  The maximum number of files to be downloaded at a
                         time. If you are downloading files in the GB this
                         number should be the same  as signers flag  [default:
                         10]
  --parts INTEGER        The maximum number of pieces a file should be divided
                         into to show progress. GB sized files should have >20
                         parts MB sized files can have only one part.
                         [default: 10]
  -v, --verbose          Enable downloading and debugging output
  --help                 Show this message and exit.
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

Now you should be ready to start coding and testing! Tests are run through the `pytest` program:

```sh
$ pytest

========================= test session starts =========================
platform darwin -- Python 3.9.4, pytest-7.2.0, pluggy-1.0.0
rootdir: /Users/beckmanl/code/drs_downloader, configfile: pyproject.toml
plugins: cov-4.0.0, anyio-3.6.2
collected 4 items

tests/test_main.py ...                                            [ 75%]
tests/unit/test_basic_cli.py .                                    [100%]

========================== 4 passed in 14.68s ==========================
```

## Authentication

In order to get the downloader to work, you need to install Google gcloud CLI on your local machine. https://cloud.google.com/sdk/docs/install

Next, you must connect the google account that your Terra account connected to to g cloud. This is done with gcloud auth login:

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

## Tests

All tests and test files are stored in the `tests` directory. Pytest is used as the testing framework. To run all tests with a coverage report run `pytest` with the `--cov=tests` flag:

```sh
$ pytest --cov=tests

========================= test session starts =========================
platform darwin -- Python 3.9.4, pytest-7.2.0, pluggy-1.0.0
rootdir: /Users/beckmanl/code/drs_downloader, configfile: pyproject.toml
plugins: cov-4.0.0, anyio-3.6.2
collected 4 items

tests/unit/test_main.py ...                                            [ 75%]
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

## Project layout

```sh
┌── LICENSE
├── README.md
├── docs
│   └── index.md          # The documentation homepage
├── drs_downloader        # Source directory
│   ├── __init__.py
│   ├── download.py       # Asynchronous file downloader used by main.py
│   └── main.py           # Terra DRS downloader
├── mkdocs.yml            # MkDocs configuration file
├── requirements-dev.txt  # Required packages for development
├── requirements.txt      # Required packages for installation
├── setup.py              # Setuptools file, used by Pyinstaller and pip
└── tests                 # All Python test and TSV files fo here
    ├── no-header.tsv
    ├── terra-data.tsv
    ├── terra-large-files.tsv
    ├── terra-small-files.tsv
    └── test_main.py
 ```

┌── LICENSE
├── README.md
├── docs
│   ├── index.md
│   └── mkdocs.yml
├── drs_downloader
│   ├── __init__.py
│   ├── __pycache__
│   ├── cli.py
│   ├── clients
│   │   ├── __init__.py
│   │   ├── __pycache__
│   │   ├── mock.py
│   │   └── terra.py
│   ├── manager.py
│   └── models.py
├── mkdocs.yml
├── requirements-dev.txt
├── requirements.txt
├── setup.py
└── tests
    ├── __pycache__
    ├── fixtures
    │   └── terra-data.tsv
    └── unit
        ├── __pycache__
        └── test_basic_cli.py
```
