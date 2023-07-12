__marqo_version__ = "0.1.0"
__marqo_release_page__ = f"https://github.com/marqo-ai/marqo/releases/tag/{__marqo_version__}"

__minimum_supported_marqo_version__ = "0.1.0"


def supported_marqo_version() -> str:
    return f"This Marqo python client is built for Marqo release ({__marqo_version__}) \n" \
           f"{__marqo_release_page__} \n" \
           f"The minimum supported Marqo version is ({__minimum_supported_marqo_version__}) \n"