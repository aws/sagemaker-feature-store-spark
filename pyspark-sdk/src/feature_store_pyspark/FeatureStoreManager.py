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

from pyspark.sql import DataFrame

from feature_store_pyspark.wrapper import SageMakerFeatureStoreJavaWrapper


class FeatureStoreManager(SageMakerFeatureStoreJavaWrapper):
    """A central manager for fature store data reporitory.

    ``ingest_data`` can be used to do batch data ingestion into the specified feature group. The input data should be in
    the format of spark DataFrame and feature_group_arn is the specified feature group's arn. To selectively ingest via
    offline/online store, flip the flag ``direct_offline_store`` according to different use cases.
    """
    _wrapped_class = "software.amazon.sagemaker.featurestore.sparksdk.FeatureStoreManager"

    def __init__(self):
        super(FeatureStoreManager, self).__init__()
        self._java_obj = self._new_java_obj(FeatureStoreManager._wrapped_class)

    def ingest_data(self, input_data_frame: DataFrame, feature_group_arn: string, direct_offline_store=False):
        """
        Batch ingest data into SageMaker FeatureStore.
        :param input_data_frame: the data frame to be ingested
        :param feature_group_arn: feature group arn
        :param direct_offline_store: boolean flag which specifies write data directlly to offline store
        :return:
        """
        return self._call_java("ingestData", input_data_frame, feature_group_arn, direct_offline_store)

    def load_feature_definitions_from_schema(self, input_data_frame: DataFrame):
        """
        Load feature definitions according to the schema of input data frame.
        :param input_data_frame: input Spark DataFrame to be loaded
        :return: list of feature definitions
        """
        java_feature_definitions = self._call_java("loadFeatureDefinitionsFromSchema", input_data_frame)
        return list(map(lambda definition: {
            "FeatureName": definition.featureName(),
            "FeatureType": definition.featureType().toString()
        }, java_feature_definitions))

    def validate_data_frame_schema(self, input_data_frame: DataFrame, feature_group_arn: string):
        """
        Validate data against SageMaker Feature Group schema.
        Throws py4j.protocol.Py4JJavaError in case of validation failure.
        :param input_data_frame: input Spark DataFrame to be validated
        :param feature_group_arn: feature group arn
        :return:
        """
        return self._call_java("validateDataFrameSchema", input_data_frame, feature_group_arn)
