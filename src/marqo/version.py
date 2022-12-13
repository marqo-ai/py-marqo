__marqo_version__ = "0.0.10"
__marqo_release_page__ = "https://github.com/marqo-ai/marqo/releases/tag/0.0.10"


def supported_marqo_version() -> str:
    return f"This Marqo python client is built for Marqo release ({__marqo_version__}) \n" \
           f"{__marqo_release_page__}"

