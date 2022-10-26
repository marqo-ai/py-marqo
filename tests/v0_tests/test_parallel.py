from marqo.client import Client
from marqo.errors import MarqoApiError, MarqoError
import copy
from tests.marqo_test import MarqoTestCase
import string
import time
import uuid

class TestAddDocumentsPara(MarqoTestCase):

    def setUp(self) -> None:
        self.generic_header = {"Content-type": "application/json"}
        
        self.client = Client(**self.client_settings)
        self.index_name_1 = "my-test-index-1"
        self.config = copy.deepcopy(self.client.config)
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass
        
        self.para_params = {'server_batch_size':10, 'processes':2}
        self.sleep = 1
        self.identifiers = [str(uuid.uuid4()) for i in range(100)] 
        self.data = [{'text':f'somethingelse{i}', 'other_text':i[::-1], '_id':i} for i in self.identifiers]


    def test_add_documents_parallel_no_create_index_get(self) -> None:

        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass

        identifiers = self.identifiers
        data = self.data

        res = self.client.index(self.index_name_1).add_documents(data, **self.para_params)

        time.sleep(self.sleep)

        # # check the document retrieved
        for _id in identifiers:
            res = self.client.index(self.index_name_1).get_document(_id)
            assert res == data[identifiers.index(_id)]

    def test_add_documents_parallel_no_create_index_search_single_field(self) -> None:

        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass

        identifiers = self.identifiers
        data = self.data

        res = self.client.index(self.index_name_1).add_documents(data, **self.para_params)

        time.sleep(self.sleep)

        # CHECK ALL
        for _id in identifiers:
            text_1 = f'somethingelse{_id}'
            res = self.client.index(self.index_name_1).search(text_1, search_method='LEXICAL', searchable_attributes=['text', 'other_text'])
            assert res['hits'][0]['text'] == text_1, f"{res}-{text_1}"

    def test_add_documents_parallel_no_create_index_search(self) -> None:

        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass

        identifiers = self.identifiers
        data = self.data

        res = self.client.index(self.index_name_1).add_documents(data, **self.para_params)

        time.sleep(self.sleep)

        # CHECK ALL
        for _id in identifiers:
            # chceck first and last
            text_1 = f'somethingelse{_id}'
            res = self.client.index(self.index_name_1).search(text_1, search_method='LEXICAL')
            assert res['hits'][0]['text'] == text_1, f"{res}-{text_1}"
