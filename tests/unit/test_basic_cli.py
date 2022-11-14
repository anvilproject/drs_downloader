import os.path


def test_license():
    """The project should have license."""
    assert os.path.isfile('./LICENSE')
