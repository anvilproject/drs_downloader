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
  - navigate to the project home directory. 
  - `pip install -e .` will install the package and keep it updated without having to re-install. 

### Development

#### Authentication

```sh
gcloud auth login
gcloud config set project <project ID>
```

https://anvil.terra.bio/#workspaces/anvil-stage-demo/DRS-downloader/data

https://anvil.terra.bio/#profile?tab=externalIdentities

Add instructions on how to become a member of our test terra project: https://anvil.terra.bio/#workspaces/anvil-stage-demo/DRS-downloader/data](https://anvil.terra.bio/#workspaces/anvil-stage-demo/DRS-downloader/data

Add instructions or a link to gcloud setup

Add instructions to link to the NHLBI BioData Catalyst Framework Services. including =google hack

## Usage

```sh
drs_downloader <Input TSV File> <DRS Header in TSV file> <Destination>
```

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
collected 6 items

tests/test_gen3_downloader.py .....                                                                              [ 83%]
tests/test_main.py .                                                                                             [100%]

---------- coverage: platform darwin, python 3.9.4-final-0 -----------
Name                            Stmts   Miss  Cover
---------------------------------------------------
tests/test_gen3_downloader.py      44     12    73%
tests/test_main.py                  8      0   100%
---------------------------------------------------
TOTAL                              52     12    77%


============================== 6 passed in 17.43s ==============================

```

## Project layout

```sh
┌── LICENSE
├── README.md
├── docs
│   └── index.md            # The documentation homepage
├── drs_downloader          # Source directory
│   ├── DRSClient.py        # DRS authenticator class
│   ├── Gen3DRSClient.py    # Gen3 authenticator class, derived from DRSClient.py
│   ├── __init__.py
│   ├── download.py         # Asynchronous file downloader, used by main.py and gen3_downloader.py
│   ├── gen3_downloader.py  # GA4GH compliant Gen3 DRS downloader
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
    ├── gen3-data.tsv
    ├── terra-data.tsv
    ├── test_gen3_downloader.py
    └── test_main.py
 ```