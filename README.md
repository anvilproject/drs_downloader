# DRS Downloader <!-- omit from toc -->

[![DRS Downloader][build-badge]][build-link]

[build-badge]: https://github.com/anvilproject/drs_downloader/actions/workflows/build.yml/badge.svg
[build-link]: https://github.com/anvilproject/drs_downloader/actions/workflows/build.yml

A file download tool for AnVIL/TDR data identified by DRS URIs

- [Installation](#installation)
  - [Checksum Verification](#checksum-verification)
    - [Successful Verification Example](#successful-verification-example)
    - [Unsuccessful Verification Example](#unsuccessful-verification-example)
- [Usage](#usage)
  - [Quick Start](#quick-start)
    - [Arguments:](#arguments)
  - [Example](#example)
  - [Help/Additional Options](#helpadditional-options)
- [Authentication](#authentication)
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

In order to verify that the downloaded file can be trusted checksums are provided in [`checksums.txt`][checksums]. See below for an example of how to use this file.

#### Successful Verification Example

To verify the integrity of the binaries on macOS run the following:

```sh
$ shasum -c checksums.txt --ignore-missing
drs-downloader-macOS.zip: OK
```

If the `shasum` command outputs `OK` than the verification was successful and the executable can be trusted.

#### Unsuccessful Verification Example

Alternatively if the commad outputs `FAILED` than the checksum did not match and the binary should not be run.

```sh
$ shasum -c checksums.txt --ignore-missing
drs-downloader-macOS.zip: FAILED
shasum: WARNING: 1 computed checksum did NOT match
shasum: checksums.txt: no file was verified
```

In such a case please reach out to the contributors for assistance.

## Usage

### Quick Start

```sh
$ drs_downloader terra -m <manifest file> -d <destination directory>
```

#### Arguments:

`-s, --silent`
: Disables all output to the terminal during and after downloading.

`-d, --destination_dir TEXT`
: The directory or folder to download the DRS Objects to. Defaults to `/tmp/testing` if no value is provided.

`-m, --manifest_path TEXT`
: The manifest file that contains the DRS Objects to be downloaded. Typically a TSV file with one row per DRS Object.

`--drs_header TEXT`
: The value of the column in the manifest file containing the DRS Object IDs. Defaults to `pfb:ga4gh_drs_uri` if no value is provided.

### Example

The below command is a basic example of how to structure a download command with all of the required arguments. It uses:

* a manifest file called [`terra-data.tsv`](https://github.com/anvilproject/drs_downloader/blob/abdb19335a076d8c127f381a5e1a116fa49c8332/tests/fixtures/terra-data.tsv) with 10 DRS Objects
* a DRS header value of `pfb:ga4gh_drs_uri` within the manifest file to reference the DRS Objects. It can be omitted since this is the default value used by the downloader.
* a directory called `DATA` as the download destination

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

Let's take a look at different manifest file called `anvil_1000_genomes_public_file_inventory.tsv`. Namely the DRS header value is now 'drs_uri'

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

1. Right click on the log in/renew button.
2. Select copy url.
3. Copy this link in another tab but instead of pressing enter go to the end of the URL that was copied
   and change the suffix of the URL from =[old suffix] to =google

If your URIs are not from bio data catalyst then authenticate with your Terra Linked Google account on the other
sites.

Now run `gcloud auth print-access-token`. This should return a long string of letters an numbers. If it doesn't then
your Terra google account is probably not linked with your gcloud account.

To test that this setup returns signed URLs copy and paste the below curl command into your terminal, but instead of running it replace [URI] with a DRS uri that belongs to a small file from your TSV file. By running this in terminal you should get back a signed URL that you can copy and paste into your browser to download a file.

```sh
curl --request POST  --url https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v3  --header "authorization: Bearer $(gcloud auth print-access-token)"  --header 'content-type: application/json'  --data '{ "url": "[URI]", "fields": ["fileName", "size", "hashes", "accessUrl"] }'
```

If you can run the above command with your own drs URI than you are setup to run the command line tool.

## Credits

This project is developed in partnership between The AnVIL Project, the Broad Institute, and the Ellrott Lab at Oregon Health & Science University. Development is lead by [Brian Walsh](https://github.com/bwalsh) with contributions from [Matthew Peterkort](https://github.com/matthewpeterkort) and [Liam Beckman](https://github.com/lbeckman314). Special thanks to Michael Baumann at the Broad Institute for guidance and development recommendations.
