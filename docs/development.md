# Development

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

## Tests

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