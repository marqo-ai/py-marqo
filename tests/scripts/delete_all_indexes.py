import os

import marqo


def cleanup_documents_from_all_indices():
    local_marqo_settings = {
        "url": os.environ.get("MARQO_URL", 'http://localhost:8882'),
    }
    api_key = os.environ.get("MARQO_API_KEY", None)
    if api_key:
        local_marqo_settings["api_key"] = api_key
    client = marqo.Client(**local_marqo_settings)
    indexes = client.get_indexes()
    for index in indexes['results']:
        if client.config.is_marqo_cloud:
            if index.get_status()["index_status"] == marqo.enums.IndexStatus.CREATED:
                index.delete()
        else:
            index.delete()


if __name__ == '__main__':
    cleanup_documents_from_all_indices()
