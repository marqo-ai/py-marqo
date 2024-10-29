__minimum_supported_marqo_version__ = "2.13.0"

# NOTE: This isn't used anywhere
def supported_marqo_version() -> str:
    return f"The minimum supported Marqo version for this client is ({__minimum_supported_marqo_version__}) \n"


def minimum_supported_marqo_version() -> str:
    return __minimum_supported_marqo_version__
