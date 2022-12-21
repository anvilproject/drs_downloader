import pytest
import os
from pathlib import Path


@pytest.fixture(autouse=True, scope="module")
def clean_current_directory():
    """Remove files created by the test functions within the current working directory.
    """

    old_cwd = os.listdir(os.getcwd())
    yield
    new_cwd = os.listdir(os.getcwd())
    added_files = [file for file in new_cwd if file not in old_cwd]
    for file in added_files:
        Path(file).unlink()
