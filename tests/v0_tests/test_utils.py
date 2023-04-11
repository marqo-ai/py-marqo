from marqo import utils
import unittest


class TestUtils(unittest.TestCase):

    def test_construct_authorized_url(self):
        assert "https://admin:admin@localhost:9200" == utils.construct_authorized_url(
            url_base="https://localhost:9200", username="admin", password="admin"
        )

    def test_construct_authorized_url_empty(self):
        assert "https://:@localhost:9200" == utils.construct_authorized_url(
            url_base="https://localhost:9200", username="", password=""
        )

    def test_translate_device_string_for_url(self):
        translations = [("cpu", "cpu"), ("CUDA", "cuda"), (None, None),
                        ("cpu:1", "cpu1"), ("CUDA:24", "cuda24"),
                        ("cuda2", "cuda2")]
        for to_be_translated, expected in translations:
            assert expected == utils.translate_device_string_for_url(to_be_translated)

    def test_convert_list_to_query_params(self):
        q = "key"
        values = ["a", "one", "c"]
        expected = "key=a&key=one&key=c"
        assert expected == utils.convert_list_to_query_params(q, values)

    def test_convert_list_to_query_params_escaped(self):
        q = "key"
        values = [
            "John Doe & Jane Smith",      # Space and ampersand
            "email@example.com",          # At sign
            "100% free",                  # Percent sign
            "file/path/name.txt",         # Slash
            "query?param=value",          # Question mark
            "color:blue",                 # Colon
            "name[0]=John",               # Square brackets
            "price=20$ (discounted)",     # Dollar sign and parentheses
            "comment=Hello, world!",      # Comma and exclamation point
            "math=3+2=5",                 # Plus sign and equal sign
            "John#Doe",                 # Hash (number) sign
            "note>important<reminder",    # Greater-than and less-than signs
            "text*bold*",                 # Asterisk
            "a^2 + b^2 = c^2",            # Caret
            "a|b|c",                      # Vertical bar
            "{x: 1, y: 2}",               # Curly braces
            "a~b",                        # Tilde (doesn't get escaped)
            "a`b",                        # Backtick (grave accent)
            "text\\example",              # Backslash
            "quote: \"hello world\"",     # Double quote
            "quote: 'hello world'",       # Single quote (apostrophe)
            "John;Doe",                 # Semicolon
        ]
        expected = "key=John+Doe+%26+Jane+Smith&key=email%40example.com&key=100%25+free&key=file%2Fpath%2Fname.txt&key=query%3Fparam%3Dvalue&key=color%3Ablue&key=name%5B0%5D%3DJohn&key=price%3D20%24+%28discounted%29&key=comment%3DHello%2C+world%21&key=math%3D3%2B2%3D5&key=John%23Doe&key=note%3Eimportant%3Creminder&key=text%2Abold%2A&key=a%5E2+%2B+b%5E2+%3D+c%5E2&key=a%7Cb%7Cc&key=%7Bx%3A+1%2C+y%3A+2%7D&key=a~b&key=a%60b&key=text%5Cexample&key=quote%3A+%22hello+world%22&key=quote%3A+%27hello+world%27&key=John%3BDoe"
        assert expected == utils.convert_list_to_query_params(q, values)
