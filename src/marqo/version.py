#__marqo_version__ = "2.9.0"
__tests_target_marqo_version__ = "2.9.0"
#__marqo_release_page__ = f"https://github.com/marqo-ai/marqo/releases/tag/{__marqo_version__}"

__minimum_supported_marqo_version__ = "2.9.0"


def supported_marqo_version() -> str:
    return f"The minimum supported Marqo version for this client is ({__minimum_supported_marqo_version__}) \n"


def minimum_supported_marqo_version() -> str:
    return __minimum_supported_marqo_version__
