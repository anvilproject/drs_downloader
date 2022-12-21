# DRS Downloader <!-- omit from toc -->

[![DRS Downloader][build-badge]][build-link]

[build-badge]: https://github.com/anvilproject/drs_downloader/actions/workflows/build.yml/badge.svg
[build-link]: https://github.com/anvilproject/drs_downloader/actions/workflows/build.yml

A file download tool for AnVIL/TDR data identified by DRS URIs


- [Installation](#installation)
    - [Checksum Verification](#checksum-verification)
    - [Requirements](#requirements)
        - [Authentication](#authentication)
- [Usage](#usage)
    - [Quick Start](#quick-start)
        - [Arguments](#arguments)
    - [Basic Example](#basic-example)
    - [Example with a Different Header Value](#example-with-a-different-header-value)
    - [Help/Additional Options](#helpadditional-options)
- [Credits](#credits)

## Installation

| Operating System | DRS Downloader                        | Checksum                       |
| ---------------- | ------------------------------------- | ------------------------------ |
| MacOS            | [drs-downloader-macOS.zip][macos]     | [checksums.txt](checksums)     |
| Linux            | [drs-downloader-Linux.zip](Linux)     | [checksums.txt](checksums.txt) |
| Windows          | [drs-downloader-Windows.zip](Windows) | [checksums.txt](checksums.txt) |

[macos]: https://github.com/anvilproject/drs_downloader/releases/latest/download/drs-downloader-macOS.zip
[linux]: https://github.com/anvilproject/drs_downloader/releases/latest/download/drs-downloader-Linux.zip
[windows]: https://github.com/anvilproject/drs_downloader/releases/latest/download/drs-downloader-Windows.zip
[checksums]: https://github.com/anvilproject/drs_downloader/releases/latest/download/checksums.txt

Download the latest `drs_downloader` zip file for your operating system. Unzipping the downloaded file will provide a `drs_downloader` executable file that can be run directly.

### Checksum Verification

In order to verify that the downloaded file can be trusted checksums are provided in [`checksums.txt`][checksums]. See below for examples of how to use this file.

<details>
<summary>Successful Verification</summary>

To verify the integrity of the binaries on macOS run the following in the same directory as the downloaded zip file:

```sh
$ shasum -c checksums.txt --ignore-missing
drs-downloader-macOS.zip: OK
```

If the `shasum` command outputs `OK` than the verification was successful and the executable can be trusted.
</details>

<details>
<summary>Unsuccessful Verification</summary>

Alternatively if the commad outputs `FAILED` than the checksum did not match and the binary should not be run.

```sh
$ shasum -c checksums.txt --ignore-missing
drs-downloader-macOS.zip: FAILED
shasum: WARNING: 1 computed checksum did NOT match
shasum: checksums.txt: no file was verified
```

In such a case please reach out to the contributors for assistance.
</details>

### Requirements

The downloader requires that a Google Cloud project be designated as the billing project. In order for the downloader to authenticate and set the desired billing project the gcloud CLI tool must first be installed:

* [gcloud CLI](https://cloud.google.com/sdk/docs/install) — used to authenticate the downloader and set the billing project.
* [Python](https://www.python.org/) — required for gcloud CLI functionality.

#### Authentication

Upon running the following `gcloud` command a browser window will open in which you may choose the Google account used for the billing project:

```sh
$ gcloud auth application-default login
Your browser has been opened to visit:

    https://accounts.google.com/o/oauth2/auth?response_type=code&client_id=...


You are now logged in as [rosalind@ohsu.edu].
Your current project is [terra-314159].  You can change this setting by running:
  $ gcloud config set project PROJECT_ID
```

To change the billing project at any time you may use either the `$ gcloud config set project PROJECT_ID` command or the built-in `drs-downloader` command:

```sh
$ drs_downloader terra --project-id Project ID>
```

## Usage

### Quick Start

```sh
$ drs_downloader terra -m <manifest file> -d <destination directory>
```

#### Arguments

`-s, --silent`
> Disables all output to the terminal during and after downloading.

`-d, --destination_dir TEXT`
> The directory or folder to download the DRS Objects to. Defaults to `/tmp/testing` if no value is provided.

`-m, --manifest_path TEXT`
> The manifest file that contains the DRS Objects to be downloaded. Typically a TSV file with one row per DRS Object.

`--drs_header TEXT`
> The value of the column in the manifest file containing the DRS Object IDs. Defaults to `pfb:ga4gh_drs_uri` if no value is provided.

### Basic Example

The below command is a basic example of how to structure a download command with all of the required arguments. It uses:

* a **manifest file** called [`terra-data.tsv`][terra-data] with 10 DRS Objects
* a **DRS header value** of `pfb:ga4gh_drs_uri` within the manifest file to reference the DRS Objects. It can be omitted since this is the default value used by the downloader.
* a **download directory** called `DATA` as the destination

[terra-data]: https://github.com/anvilproject/drs_downloader/blob/feature/download-recovery/tests/fixtures/manifests/terra-data.tsv


```sh
$ drs_downloader terra -m tests/fixtures/manifest/terra-data.tsv -d DATA
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
```

After the download completes we can look in the `DATA` directory to confirm that all 10 DRS Objects have been downloaded:

```sh
$ ls ./DATA
HG00536.final.cram.crai HG01552.final.cram.crai
HG02450.final.cram.crai HG04209.final.cram.crai
NA20356.final.cram.crai HG00622.final.cram.crai
HG02142.final.cram.crai HG03873.final.cram.crai
NA18613.final.cram.crai NA20525.final.cram.crai
```

### Example with a Different Header Value

Let's take a look at different manifest file called [`terra-different-header.tsv`][terra-different-header]. Namely the DRS header value is now `drs_uri` so we will need to tell the downloader which column to find the DRS URI's in the manifest with the `--drs_header` flag:

```sh
drs_downloader terra -m tests/fixtures/manifests/terra-different-header.tsv -d DATA --drs_header drs_uri
```

This will download the DRS Objects specified in the `drs_uri` column into the `DATA` directory just as before.

[terra-different-header]: https://github.com/anvilproject/drs_downloader/blob/feature/download-recovery/tests/fixtures/manifests/terra-different-header.tsv 

### Help/Additional Options

To see all available flags run the `help` command:

```sh
$ drs_downloader terra --help

Usage: drs_download terra [OPTIONS]

  Copy files from terra.bio

Options:
  -s, --silent                Display nothing.
  -d, --destination_dir TEXT  Destination directory.  [default: /tmp/testing]
  -m, --manifest_path TEXT    Path to manifest tsv.
  --drs_header TEXT           The column header in the TSV file associated
                              with the DRS URIs.Example: pfb:ga4gh_drs_uri
  --help                      Show this message and exit.
```

## Credits

This project is developed in partnership between The AnVIL Project, the Broad Institute, and the Ellrott Lab at Oregon Health & Science University. Development is lead by [Brian Walsh](https://github.com/bwalsh) with contributions from [Matthew Peterkort](https://github.com/matthewpeterkort) and [Liam Beckman](https://github.com/lbeckman314). Special thanks to Michael Baumann at the Broad Institute for guidance and development recommendations.
