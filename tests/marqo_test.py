import unittest
from marqo.utils import construct_authorized_url


class MarqoTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        local_marqo_settings = {
            "url": 'http://localhost:8882',
            "main_user": "admin",
            "main_password": "admin"
        }
        cls.client_settings = local_marqo_settings
        cls.authorized_url = construct_authorized_url(
            url_base=cls.client_settings["url"],
            username=cls.client_settings["main_user"],
            password=cls.client_settings["main_password"]
        )
