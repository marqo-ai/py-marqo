from pytest import mark

from marqo.enums import InterpolationMethod
from marqo.errors import MarqoWebError
from tests.marqo_test import MarqoTestCase
from tests.cloud_test_logic.cloud_test_index import CloudTestIndex

@mark.fixed
class TestRecommend(MarqoTestCase):

    def test_recommend_defaults(self):
        """
        Test recommend with only required fields provided
        """
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )

            docs = [
                {
                    "_id": "1",
                    "Title": "Red orchid",
                    "tags": ["flower", "orchid"],
                },
                {
                    "_id": "2",
                    "Title": "Red rose",
                    "tags": ["flower"],
                },
                {
                    "_id": "3",
                    "Title": "Europe",
                    "tags": ["continent"],
                },
            ]

            add_docs_results = self.client.index(test_index_name).add_documents(docs, tensor_fields=["Title"])

            if add_docs_results["errors"]:
                raise Exception(f"Failed to add documents to index {test_index_name}")

            res = self.client.index(test_index_name).recommend(
                documents=['1', '2']
            )

            ids = [doc["_id"] for doc in res["hits"]]

            self.assertEqual(set(ids), {"3"})

    def test_recommend_allFields(self):
        """
        Test recommend with all fields provided
        """

        self.test_cases = [(CloudTestIndex.structured_text, self.structured_index_name), ]
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )

            docs = [
                {
                    "_id": "1",
                    "text_field_1": "Red orchid",
                    "text_field_2": "flower",
                },
                {
                    "_id": "2",
                    "text_field_1": "Red rose",
                    "text_field_2": "flower"
                },
                {
                    "_id": "3",
                    "text_field_1": "Europe",
                    "text_field_2": "continent",
                }
            ]

            add_docs_results = self.client.index(test_index_name).add_documents(docs)

            if add_docs_results["errors"]:
                raise Exception(f"Failed to add documents to index {test_index_name}")

            res = self.client.index(test_index_name).recommend(
                documents=['1', '2'],
                tensor_fields=["text_field_1"],
                interpolation_method=InterpolationMethod.SLERP,
                exclude_input_documents=True,
                limit=10,
                offset=0,
                ef_search=100,
                approximate=True,
                searchable_attributes=["text_field_1"],
                show_highlights=True,
                # reranker='google/owlvit-base-patch32', can't rerank text
                filter_string='text_field_2:(continent)',
                attributes_to_retrieve=["text_field_1"],
                score_modifiers={
                    "multiply_score_by":
                        [
                            {
                                "field_name": "int_filter_field_1",
                                "weight": 1
                            }
                        ]
                }
            )

            ids = [doc["_id"] for doc in res["hits"]]

            self.assertEqual(set(ids), {"3"})

    def test_recommend_rerank_depth_behavior(self):
        """API-level test that rerank_depth affects recommender results according to expected behavior."""
        self.test_cases = [(CloudTestIndex.structured_text, self.structured_index_name)]
        docs = [
            {"_id": "doc_0","text_field_1": "Project Overview","text_field_2": "Summary of the project’s goals and deliverables."},
            {"_id": "doc_1", "text_field_1": "Team Roles", "text_field_2": "Descriptions of each team member’s responsibilities."},
            {"_id": "doc_2", "text_field_1": "Timeline", "text_field_2": "Key milestones and deadlines for the project."},
            {"_id": "doc_3", "text_field_1": "Budget Estimate", "text_field_2": "Projected costs and resource allocation."},
            {"_id": "doc_4", "text_field_1": "Tech Stack", "text_field_2": "Overview of technologies and tools being used."},
            {"_id": "doc_5", "text_field_1": "Risk Assessment", "text_field_2": "Potential risks and mitigation strategies."},
            {"_id": "doc_6", "text_field_1": "Client Feedback", "text_field_2": "Summary of feedback received from stakeholders."},
            {"_id": "doc_7", "text_field_1": "Testing Plan", "text_field_2": "Details on testing strategies and coverage."},
            {"_id": "doc_8", "text_field_1": "Deployment Guide", "text_field_2": "Steps and procedures for deploying the application."},
            {"_id": "doc_9", "text_field_1": "Post-Mortem", "text_field_2": "Analysis of what went well and areas for improvement."},
            {"_id": "doc_10", "text_field_1": "Design Specs", "text_field_2": "Detailed technical specifications for components."},
            {"_id": "doc_11", "text_field_1": "Security Plan", "text_field_2": "Guidelines for maintaining security throughout the project."},
            {"_id": "doc_12", "text_field_1": "Maintenance Schedule", "text_field_2": "Outline of scheduled maintenance tasks and timelines."},
            {"_id": "doc_13", "text_field_1": "Release Notes", "text_field_2": "Documented changes and updates included in each release."},
            {"_id": "doc_14", "text_field_1": "Integration Plan", "text_field_2": "Steps to integrate with existing systems and services."},
            {"_id": "doc_15", "text_field_1": "User Research", "text_field_2": "Findings from usability tests and user interviews."},
            {"_id": "doc_16", "text_field_1": "Requirements Document", "text_field_2": "Comprehensive list of requirements and constraints."},
            {"_id": "doc_17", "text_field_1": "System Architecture", "text_field_2": "High-level overview of system components and relationships."},
            {"_id": "doc_18", "text_field_1": "Data Migration Plan", "text_field_2": "Plan for transferring data from legacy systems."},
            {"_id": "doc_19", "text_field_1": "Support Strategy", "text_field_2": "Approach for providing user support post-deployment."},
            {"_id": "doc_20", "text_field_1": "Project Overview", "text_field_2": "Summary of the project’s goals and deliverables."},
            {"_id": "doc_21", "text_field_1": "Team Roles", "text_field_2": "Descriptions of each team member’s responsibilities."},
            {"_id": "doc_22", "text_field_1": "Timeline", "text_field_2": "Key milestones and deadlines for the project."},
            {"_id": "doc_23", "text_field_1": "Budget Estimate", "text_field_2": "Projected costs and resource allocation."},
            {"_id": "doc_24", "text_field_1": "Tech Stack", "text_field_2": "Overview of technologies and tools being used."},
            {"_id": "doc_25", "text_field_1": "Risk Assessment", "text_field_2": "Potential risks and mitigation strategies."},
            {"_id": "doc_26", "text_field_1": "Client Feedback", "text_field_2": "Summary of feedback received from stakeholders."},
            {"_id": "doc_27", "text_field_1": "Testing Plan", "text_field_2": "Details on testing strategies and coverage."},
            {"_id": "doc_28", "text_field_1": "Deployment Guide", "text_field_2": "Steps and procedures for deploying the application."},
            {"_id": "doc_29", "text_field_1": "Post-Mortem", "text_field_2": "Analysis of what went well and areas for improvement."},
            {"_id": "doc_30", "text_field_1": "Design Specs", "text_field_2": "Detailed technical specifications for components."},
            {"_id": "doc_31", "text_field_1": "Security Plan", "text_field_2": "Guidelines for maintaining security throughout the project."},
            {"_id": "doc_32", "text_field_1": "Maintenance Schedule", "text_field_2": "Outline of scheduled maintenance tasks and timelines."},
            {"_id": "doc_33", "text_field_1": "Release Notes", "text_field_2": "Documented changes and updates included in each release."},
            {"_id": "doc_34", "text_field_1": "Integration Plan", "text_field_2": "Steps to integrate with existing systems and services."},
            {"_id": "doc_35", "text_field_1": "User Research", "text_field_2": "Findings from usability tests and user interviews."},
            {"_id": "doc_36", "text_field_1": "Requirements Document", "text_field_2": "Comprehensive list of requirements and constraints."},
            {"_id": "doc_37", "text_field_1": "System Architecture", "text_field_2": "High-level overview of system components and relationships."},
            {"_id": "doc_38", "text_field_1": "Data Migration Plan", "text_field_2": "Plan for transferring data from legacy systems."},
            {"_id": "doc_39", "text_field_1": "Support Strategy", "text_field_2": "Approach for providing user support post-deployment."},
            {"_id": "doc_40", "text_field_1": "Project Overview", "text_field_2": "Summary of the project’s goals and deliverables."},
            {"_id": "doc_41", "text_field_1": "Team Roles", "text_field_2": "Descriptions of each team member’s responsibilities."},
            {"_id": "doc_42", "text_field_1": "Timeline", "text_field_2": "Key milestones and deadlines for the project."},
            {"_id": "doc_43", "text_field_1": "Budget Estimate", "text_field_2": "Projected costs and resource allocation."},
            {"_id": "doc_44", "text_field_1": "Tech Stack", "text_field_2": "Overview of technologies and tools being used."},
            {"_id": "doc_45", "text_field_1": "Risk Assessment", "text_field_2": "Potential risks and mitigation strategies."},
            {"_id": "doc_46", "text_field_1": "Client Feedback", "text_field_2": "Summary of feedback received from stakeholders."},
            {"_id": "doc_47", "text_field_1": "Testing Plan", "text_field_2": "Details on testing strategies and coverage."},
            {"_id": "doc_48", "text_field_1": "Deployment Guide", "text_field_2": "Steps and procedures for deploying the application."},
            {"_id": "doc_49", "text_field_1": "Post-Mortem", "text_field_2": "Analysis of what went well and areas for improvement."},
            {"_id": "doc_50", "text_field_1": "Design Specs", "text_field_2": "Detailed technical specifications for components."},
            {"_id": "doc_51", "text_field_1": "Security Plan", "text_field_2": "Guidelines for maintaining security throughout the project."},
            {"_id": "doc_52", "text_field_1": "Maintenance Schedule", "text_field_2": "Outline of scheduled maintenance tasks and timelines."},
            {"_id": "doc_53", "text_field_1": "Release Notes", "text_field_2": "Documented changes and updates included in each release."},
            {"_id": "doc_54", "text_field_1": "Integration Plan", "text_field_2": "Steps to integrate with existing systems and services."},
            {"_id": "doc_55", "text_field_1": "User Research", "text_field_2": "Findings from usability tests and user interviews."},
            {"_id": "doc_56", "text_field_1": "Requirements Document", "text_field_2": "Comprehensive list of requirements and constraints."},
            {"_id": "doc_57", "text_field_1": "System Architecture", "text_field_2": "High-level overview of system components and relationships."},
            {"_id": "doc_58", "text_field_1": "Data Migration Plan", "text_field_2": "Plan for transferring data from legacy systems."},
            {"_id": "doc_59", "text_field_1": "Support Strategy", "text_field_2": "Approach for providing user support post-deployment."},
            {"_id": "doc_60", "text_field_1": "Project Overview", "text_field_2": "Summary of the project’s goals and deliverables."},
            {"_id": "doc_61", "text_field_1": "Team Roles", "text_field_2": "Descriptions of each team member’s responsibilities."},
            {"_id": "doc_62", "text_field_1": "Timeline", "text_field_2": "Key milestones and deadlines for the project."},
            {"_id": "doc_63", "text_field_1": "Budget Estimate", "text_field_2": "Projected costs and resource allocation."},
            {"_id": "doc_64", "text_field_1": "Tech Stack", "text_field_2": "Overview of technologies and tools being used."},
            {"_id": "doc_65", "text_field_1": "Risk Assessment", "text_field_2": "Potential risks and mitigation strategies."},
            {"_id": "doc_66", "text_field_1": "Client Feedback", "text_field_2": "Summary of feedback received from stakeholders."},
            {"_id": "doc_67", "text_field_1": "Testing Plan", "text_field_2": "Details on testing strategies and coverage."},
            {"_id": "doc_68", "text_field_1": "Deployment Guide", "text_field_2": "Steps and procedures for deploying the application."},
            {"_id": "doc_69", "text_field_1": "Post-Mortem", "text_field_2": "Analysis of what went well and areas for improvement."},
            {"_id": "doc_70", "text_field_1": "Design Specs", "text_field_2": "Detailed technical specifications for components."},
            {"_id": "doc_71", "text_field_1": "Security Plan", "text_field_2": "Guidelines for maintaining security throughout the project."},
            {"_id": "doc_72", "text_field_1": "Maintenance Schedule", "text_field_2": "Outline of scheduled maintenance tasks and timelines."},
            {"_id": "doc_73", "text_field_1": "Release Notes", "text_field_2": "Documented changes and updates included in each release."},
            {"_id": "doc_74", "text_field_1": "Integration Plan", "text_field_2": "Steps to integrate with existing systems and services."},
            {"_id": "doc_75", "text_field_1": "User Research", "text_field_2": "Findings from usability tests and user interviews."},
            {"_id": "doc_76", "text_field_1": "Requirements Document", "text_field_2": "Comprehensive list of requirements and constraints."},
            {"_id": "doc_77", "text_field_1": "System Architecture", "text_field_2": "High-level overview of system components and relationships."},
            {"_id": "doc_78", "text_field_1": "Data Migration Plan", "text_field_2": "Plan for transferring data from legacy systems."},
            {"_id": "doc_79", "text_field_1": "Support Strategy", "text_field_2": "Approach for providing user support post-deployment."},
            {"_id": "doc_80", "text_field_1": "Project Overview", "text_field_2": "Summary of the project’s goals and deliverables."},
            {"_id": "doc_81", "text_field_1": "Team Roles", "text_field_2": "Descriptions of each team member’s responsibilities."},
            {"_id": "doc_82", "text_field_1": "Timeline", "text_field_2": "Key milestones and deadlines for the project."},
            {"_id": "doc_83", "text_field_1": "Budget Estimate", "text_field_2": "Projected costs and resource allocation."},
            {"_id": "doc_84", "text_field_1": "Tech Stack", "text_field_2": "Overview of technologies and tools being used."},
            {"_id": "doc_85", "text_field_1": "Risk Assessment", "text_field_2": "Potential risks and mitigation strategies."},
            {"_id": "doc_86", "text_field_1": "Client Feedback", "text_field_2": "Summary of feedback received from stakeholders."},
            {"_id": "doc_87", "text_field_1": "Testing Plan", "text_field_2": "Details on testing strategies and coverage."},
            {"_id": "doc_88", "text_field_1": "Deployment Guide", "text_field_2": "Steps and procedures for deploying the application."},
            {"_id": "doc_89", "text_field_1": "Post-Mortem", "text_field_2": "Analysis of what went well and areas for improvement."},
            {"_id": "doc_90", "text_field_1": "Design Specs", "text_field_2": "Detailed technical specifications for components."},
            {"_id": "doc_91", "text_field_1": "Security Plan", "text_field_2": "Guidelines for maintaining security throughout the project."},
            {"_id": "doc_92", "text_field_1": "Maintenance Schedule", "text_field_2": "Outline of scheduled maintenance tasks and timelines."},
            {"_id": "doc_93", "text_field_1": "Release Notes", "text_field_2": "Documented changes and updates included in each release."},
            {"_id": "doc_94", "text_field_1": "Integration Plan", "text_field_2": "Steps to integrate with existing systems and services."},
            {"_id": "doc_95", "text_field_1": "User Research", "text_field_2": "Findings from usability tests and user interviews."},
            {"_id": "doc_96", "text_field_1": "Requirements Document", "text_field_2": "Comprehensive list of requirements and constraints."},
            {"_id": "doc_97", "text_field_1": "System Architecture", "text_field_2": "High-level overview of system components and relationships."},
            {"_id": "doc_98", "text_field_1": "Data Migration Plan", "text_field_2": "Plan for transferring data from legacy systems."},
            {"_id": "doc_99", "text_field_1": "Support Strategy", "text_field_2": "Approach for providing user support post-deployment."}
]

        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )

            self.client.index(test_index_name).add_documents(docs)

            add_docs_results = self.client.index(test_index_name).add_documents(docs)
            if add_docs_results["errors"]:
                raise Exception(f"Failed to add documents to index {test_index_name}")

            # Case 1: result_count < rerank_depth → limit is respected
            with self.subTest(case="result_count_less_than_rerank_depth"):
                res = self.client.index(test_index_name).recommend(
                    documents=["doc_0", "doc_1"], limit=3, offset=0, rerank_depth=5
                )
                self.assertEqual(len(res["hits"]), 3)

            # Case 2: offset > rerank_depth — offset + limit is higher, result must be present
            with self.subTest(case="offset_beyond_rerank_depth"):
                res = self.client.index(test_index_name).recommend(
                    documents=["doc_0", "doc_1"], limit=1, offset=3, rerank_depth=2
                )
                self.assertEqual(len(res["hits"]), 1)

            # Case 3: offset + result_count <= rerank_depth → return all requested hits
            with self.subTest(case="offset_within_rerank_depth"):
                res = self.client.index(test_index_name).recommend(
                    documents=["doc_0", "doc_1"], limit=2, offset=2, rerank_depth=5
                )
                self.assertEqual(len(res["hits"]), 2)

            # Case 4: rerank_depth < result_count → result_count overrides rerank_depth
            with self.subTest(case="result_count_exceeds_rerank_depth"):
                res = self.client.index(test_index_name).recommend(
                    documents=["doc_0", "doc_1"], limit=5, offset=0, rerank_depth=3
                )
                self.assertEqual(len(res["hits"]), 5)

            # Case 5: ef_search < rerank_depth → ef_search limits rerank pool
            with self.subTest(case="ef_search_limits_rerank_pool"):
                res = self.client.index(test_index_name).recommend(
                    documents=["doc_0", "doc_1"], limit=10, offset=0, rerank_depth=5, ef_search=3,
                    searchable_attributes=['text_field_1']
                )
                # expected result depends on number of shards used in the index
                if self.client.config.is_marqo_cloud:
                    expected_value = 6 # 2 shards in cloud
                else:
                    expected_value = 3 # 1 shard in open source
                self.assertLessEqual(len(res["hits"]), expected_value)

            # Case 6: rerank_depth is negative → should raise error
            with self.subTest(case="invalid_negative_rerank_depth"):
                with self.assertRaises(MarqoWebError):
                    self.client.index(test_index_name).recommend(
                        documents=["doc_0", "doc_1"], limit=10, offset=0, rerank_depth=-1
                    )
