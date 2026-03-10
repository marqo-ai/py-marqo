from unittest.mock import patch

from marqo.errors import MarqoWebError
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from pytest import mark


@mark.fixed
class TestModelCacheManagement(MarqoTestCase):
    MODEL = "open_clip/ViT-B-32/laion2b_s34b_b79k"

    # NOTE: The cuda should already have model loaded in the startup
    def test_get_cuda_info(self) -> None:
        try:
            for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
                test_index_name = self.get_test_index_name(
                    cloud_test_index_to_use=cloud_test_index_to_use,
                    open_source_test_index_name=open_source_test_index_name
                )
                res = self.client.index(test_index_name).get_cuda_info()
                if "cuda_devices" not in res:
                    raise AssertionError
        # catch error if no cuda device in marqo
        except MarqoWebError:
            pass

    def test_get_cpu_info(self) -> None:
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            res = self.client.index(test_index_name).get_cpu_info()

            if "cpu_usage_percent" not in res:
                raise AssertionError

            if "memory_used_percent" not in res:
                raise AssertionError

            if "memory_used_gb" not in res:
                raise AssertionError

    def test_get_loaded_models(self) -> None:
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            res = self.client.index(test_index_name).get_loaded_models()

            self.assertIn("models", res)
            self.assertGreaterEqual(len(res["models"]), 1)

    def test_eject_all_models(self) -> None:
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            res = self.client.index(test_index_name).get_loaded_models()
            for model in res["models"]:
                self.client.index(test_index_name).eject_model(model["modelName"])
            res = self.client.index(test_index_name).get_loaded_models()
            self.assertEqual(0, len(res["models"]))

    def test_eject_no_cached_model(self) -> None:
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            res = self.client.index(test_index_name).eject_model("void_model")
            self.assertEqual("success", res["result"])
            self.assertIn("ejected successfully", res["message"].lower())

    def test_eject_model(self) -> None:
        if self.IS_MULTI_INSTANCE:
            self.skipTest("Test will sometimes fail on marqo multi instance setup")

        settings = {"model": self.MODEL}

        if self.client.config.is_marqo_cloud:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=CloudTestIndex.unstructured_image,
                open_source_test_index_name=None
            )
            MODEL = self.client.index(test_index_name).get_settings()['model']
        else:
            self.client.create_index(self.generic_test_index_name, settings_dict=settings)
            test_index_name = self.generic_test_index_name
        d1 = {
            "doc_title": "Cool Document 1",
            "field_1": "some extra info"
        }
        self.client.index(test_index_name).add_documents([d1], device="cpu", tensor_fields=["doc_title", "field_1"])
        res = self.client.index(test_index_name).eject_model(
            self.MODEL if not self.client.config.is_marqo_cloud else MODEL
        )

        self.assertEqual("success", res["result"])
        self.assertIn("ejected successfully", res["message"].lower())

        if not self.client.config.is_marqo_cloud:
            self.client.delete_index(test_index_name)