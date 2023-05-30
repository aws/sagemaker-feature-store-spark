# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#   http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

import string
from typing import List
from pyspark.sql import DataFrame

from feature_store_pyspark.wrapper import SageMakerFeatureStoreJavaWrapper


class FeatureStoreManager(SageMakerFeatureStoreJavaWrapper):
    """A central manager for fature store data reporitory.

    ``ingest_data`` can be used to do batch data ingestion into the specified feature group. The input data should be in
    the format of spark DataFrame and feature_group_arn is the specified feature group's arn. To selectively ingest to
    offline/online store, specify the ``target_stores`` according to different use cases.
    """
    _wrapped_class = "software.amazon.sagemaker.featurestore.sparksdk.FeatureStoreManager"

    def __init__(self, assume_role_arn: string = None):
        super(FeatureStoreManager, self).__init__()
        self._java_obj = self._new_java_obj(FeatureStoreManager._wrapped_class, assume_role_arn)

    def ingest_data(self, input_data_frame: DataFrame, feature_group_arn: str, target_stores: List[str] = None):
        """
        Batch ingest data into SageMaker FeatureStore.

        :param input_data_frame (DataFrame): the DataFrame to be ingested.
        :param feature_group_arn (str): target feature group arn.
        :param target_stores (List[str]): a list of target stores which the data should be ingested to.

        :return:
        """
        return self._call_java("ingestDataInJava", input_data_frame, feature_group_arn, target_stores)

    def load_feature_definitions_from_schema(self, input_data_frame: DataFrame):
        """
        Load feature definitions according to the schema of input DataFrame.

        :param input_data_frame (DataFrame): input Spark DataFrame to be loaded.

        :return: list of feature definitions loaded from input DataFrame.
        """
        java_feature_definitions = self._call_java("loadFeatureDefinitionsFromSchema", input_data_frame)
        return list(map(lambda definition: {
            "FeatureName": definition.featureName(),
            "FeatureType": definition.featureType().toString()
        }, java_feature_definitions))

    def get_failed_stream_ingestion_data_frame(self) -> DataFrame:
        """
        Retrieve DataFrame which includes all records fail to be ingested via ``ingest_data`` method.

        :return: the DataFrame of records that fail to be ingested.
        """
        return self._call_java("getFailedStreamIngestionDataFrame")
