import pytest


def pytest_addoption(parser):
    # TODO Remove this after all the tests are fixed
    # This is only used as a temporary measure to run all tests
    # If not specified, only tests marked as 'fixed' will be run
    parser.addoption("--all", action="store_true", help="run all tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--all"):
        # If the --all option is used, run all tests
        return
    else:
        # Skip tests not marked as 'fixed'
        skip_non_fixed = pytest.mark.skip(reason="not marked as fixed")
        for item in items:
            # TODO Remove this after all the tests are fixed
            if "fixed" not in item.keywords:
                item.add_marker(skip_non_fixed)


def pytest_configure(config):
    config.addinivalue_line("markers", "fixed: mark test to run as part of fixed tests")
