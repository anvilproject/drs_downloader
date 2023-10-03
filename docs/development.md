# Development

To get ready for development first get the code:

```sh
$ git clone https://github.com/anvilproject/drs_downloader
$ cd drs_downloader
```

Then create and activate a virtual environment using `Python3.10`:

```sh
$ python3.10 -m venv venv
$ . venv/bin/activate
$ pip install -r requirements.txt -r requirements-dev.txt
```

## Terra Authentication

In order for the downloader to work, you will need to install Google gcloud CLI on your local machine. https://cloud.google.com/sdk/docs/install

Next, you must connect the google account that your Terra account connected to gcloud. This is done with gcloud auth login:

```sh
$ gcloud auth login
```

You need to have a terra project that is set up for billing. Once you get one, go to your terra workspaces page: https://anvil.terra.bio/#workspaces/

Click on the project that you want to bill to. On the righthand corner of the screen click on Cloud Information heading.

Copy and paste the Google Project Id field into the below command:

```sh
$ gcloud config set project <project ID>
```

Next, you need to link your Google account to the location where the DRS URIs will download from. This is endpoint specific.

Go to [anvil.terra.bio profile](https://anvil.terra.bio/#profile?tab=externalIdentities) page

If you are logging into bio data catalyst do the following:

1. Right click on the log in/renew button.
2. Select copy url.
3. Copy this link in another tab but instead of pressing enter go to the end of the URL that was copied and change the suffix of the URL from `=[old suffix]` to `=google`

If your URIs are not from bio data catalyst then authenticate with your Terra Linked Google account on the other sites.

Now run `gcloud auth print-access-token`. This should return a long string of letters an numbers. If it doesn't then your Terra google account is probably not linked with your gcloud account.

To test that this setup returns signed URLs copy and paste the below curl command into your terminal, but instead of running it replace [URI] with a DRS uri that belongs to a small file from your TSV file. By running this in terminal you should get back a signed URL that you can copy and paste into your browser to download a file.

```sh
$ curl --request POST  --url https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v3  --header "authorization: Bearer $(gcloud auth print-access-token)"  --header 'content-type: application/json'  --data '{ "url": "[URI]", "fields": ["fileName", "size", "hashes", "accessUrl"] }'
```

If you can run the above command with your own DRS URI than you are setup to run the command line tool.

Now you should be ready to start coding and testing!

## Gen3 Authentication

## Tests

All tests and test files are stored in the `tests` directory. [Pytest](https://pytest.org/) is used as the testing framework. To run all tests with a coverage report run `pytest` with the `--cov=tests` flag:

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

## Contributing

Pull requests, issues, and feature requests welcome. Please reach out if you have questions setting the development environment!

## Project layout

```
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

## Useful Packages

Here are a few packages we've found useful for this project:

Pip Packages
- [pipdeptree](https://pypi.org/project/pipdeptree/)
- [Flake8](https://pypi.org/project/flake8/)
- [autopep8](https://pypi.org/project/autopep8/)

Git Extensions
- [pre-commit](https://pre-commit.com/)
- [git-secrets](https://github.com/awslabs/git-secrets)

VS Code Extensions
- [autoDocstring](https://marketplace.visualstudio.com/items?itemName=njpwerner.autodocstring)
- [Markdown All in One](https://marketplace.visualstudio.com/items?itemName=yzhang.markdown-all-in-one)
- [Python](https://marketplace.visualstudio.com/items?itemName=ms-python.python)
