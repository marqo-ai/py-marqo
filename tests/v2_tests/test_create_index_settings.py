import unittest
from unittest.mock import patch

from marqo.models.create_index_settings import CreateIndexSettings


class TestCreateIndexSettings(unittest.TestCase):
    def test_default_settings(self):
        settings = CreateIndexSettings()
        assert settings.treat_urls_and_pointers_as_images is False
        assert settings.model is None
        assert settings.normalize_embeddings is True
        assert settings.sentences_per_chunk == 2
        assert settings.sentence_overlap == 0
        assert settings.image_preprocessing_method is None
        assert settings.settings_dict is None
        assert settings.inference_node_type is None
        assert settings.storage_node_type is None
        assert settings.inference_node_count == 1
        assert settings.storage_node_count == 1
        assert settings.replicas_count == 0
        assert settings.wait_for_readiness is True
        assert settings.inference_type is None
        assert settings.storage_class is None
        assert settings.number_of_inferences == 1
        assert settings.number_of_shards == 1
        assert settings.number_of_replicas == 0

    def test_deprecated_settings_passed_raise_warning(self):
        with patch("marqo.marqo_cloud_instance_mappings.mq_logger.warning") as mock_warning:
            settings = CreateIndexSettings(
                inference_node_type="marqo.CPU",
                storage_node_type="marqo.basic",
            )
            assert settings.inference_type == "marqo.CPU"
            assert settings.storage_class == "marqo.basic"
            assert mock_warning.call_count == 1

    def test_deprecated_settings_passed_and_new_ones_raise_exception(self):
        with self.assertRaises(ValueError):
            settings = CreateIndexSettings(
                inference_node_type="marqo.CPU",
                storage_node_type="marqo.basic",
                inference_type="marqo.CPU",
            )

    def test_settings_dict_passed_with_other_settings_raise_exception(self):
        with self.assertRaises(ValueError):
            settings = CreateIndexSettings(
                settings_dict={"index_defaults": {"model": "hf/all_datasets_v4_MiniLM-L6"}},
                inference_node_type="marqo.CPU",
            )

    def test_settings_dict_passed_with_setting_that_can_be_passed_does_not_raise_exception(self):
        settings = CreateIndexSettings(
            settings_dict={"index_defaults": {"model": "hf/all_datasets_v4_MiniLM-L6"}},
            wait_for_readiness=False,
        )
        assert settings.settings_dict == {"index_defaults": {"model": "hf/all_datasets_v4_MiniLM-L6"}}

    def test_deprecate_parameters_set_values_for_new_parameters(self):
        settings = CreateIndexSettings(
            inference_node_type="marqo.CPU",
            storage_node_type="marqo.basic",
            replicas_count=1,
            inference_node_count=2,
            storage_node_count=2,
        )
        assert settings.inference_type == "marqo.CPU"
        assert settings.storage_class == "marqo.basic"
        assert settings.number_of_inferences == 2
        assert settings.number_of_shards == 2
        assert settings.number_of_replicas == 1
        assert settings.storage_node_type is None
        assert settings.inference_node_type is None

    def test_pass_all_valid_parameters_except_settings_dict(self):
        settings = CreateIndexSettings(
            treat_urls_and_pointers_as_images=True,
            model="hf/all_datasets_v4_MiniLM-L6",
            normalize_embeddings=False,
            sentences_per_chunk=3,
            sentence_overlap=1,
            image_preprocessing_method="patch",
            wait_for_readiness=False,
            inference_type="marqo.CPU",
            storage_class="marqo.basic",
            number_of_inferences=2,
            number_of_shards=2,
            number_of_replicas=1,
        )
        assert settings.treat_urls_and_pointers_as_images is True
        assert settings.model == "hf/all_datasets_v4_MiniLM-L6"
        assert settings.normalize_embeddings is False
        assert settings.sentences_per_chunk == 3
        assert settings.sentence_overlap == 1
        assert settings.image_preprocessing_method == "patch"
        assert settings.settings_dict is None
        assert settings.wait_for_readiness is False
        assert settings.inference_type == "marqo.CPU"
        assert settings.storage_class == "marqo.basic"
        assert settings.number_of_inferences == 2
        assert settings.number_of_shards == 2
        assert settings.number_of_replicas == 1


