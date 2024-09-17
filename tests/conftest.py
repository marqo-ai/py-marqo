import pytest


def pytest_addoption(parser):
    # TODO Remove this after all the tests are fixed
    # This is only used as a temporary measure to run all tests
    # If not specified, only tests marked as 'fixed' will be run
    parser.addoption("--all", action="store_true", default=False, help="run all tests")
    parser.addoption("--cloud", action="store_true", default=False, help="run pytest in the cloud mode. Skip tests"
                                                                         "marked as 'local_only_tests'.")
    parser.addoption("--local", action="store_true", default=True, help="run pytest in the local mode. Skip tests"
                                                                        "marked as 'cloud_only_tests'.")


def pytest_collection_modifyitems(config, items):
    # If --all option is specified, run all tests
    if config.getoption("--all"):
        return

    # Skip tests not marked as 'fixed'
    skip_non_fixed = pytest.mark.skip(reason="not marked as fixed")
    # Skip tests marked as 'cloud_only_tests' when in local mode
    skip_cloud_only = pytest.mark.skip(reason="skipped in local mode")
    # Skip tests marked as 'local_only_tests' when in cloud mode
    skip_local_only = pytest.mark.skip(reason="skipped in cloud mode")

    # If --cloud is provided, set --local to False
    if config.getoption("--cloud"):
        config.option.local = False

    for item in items:
        if "fixed" not in item.keywords:
            item.add_marker(skip_non_fixed)

        # If running in cloud mode, skip local-only tests
        if config.getoption("--cloud"):
            if "local_only_tests" in item.keywords:
                item.add_marker(skip_local_only)

        # If running in local mode, skip cloud-only tests
        if config.getoption("--local"):
            if "cloud_only_tests" in item.keywords:
                item.add_marker(skip_cloud_only)


def pytest_configure(config):
    # Register custom markers programmatically
    config.addinivalue_line("markers", "local_only_tests: mark a test to run only in local mode")
    config.addinivalue_line("markers", "cloud_only_tests: mark a test to run only in cloud mode")
    config.addinivalue_line("markers", "fixed: mark test to run as part of fixed tests")

