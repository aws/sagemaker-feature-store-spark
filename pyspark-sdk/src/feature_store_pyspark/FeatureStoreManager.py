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

import pyspark
from pyspark.sql import DataFrame

from feature_store_pyspark.wrapper import SageMakerFeatureStoreJavaWrapper


def _is_pyspark_35_or_newer() -> bool:
    """Return True if the installed PySpark version is >= 3.5.

    Lake Formation credential vending support in the Scala JAR is only built for
    Spark 3.5+. This helper lets ingest_data gate that feature at runtime.
    Uses only stdlib + pyspark to avoid adding a ``packaging`` dependency.
    """
    try:
        major_minor = tuple(int(p) for p in pyspark.__version__.split(".")[:2])
    except (ValueError, AttributeError):
        # If the version string is malformed, err on the safe side (treat as older).
        return False
    return major_minor >= (3, 5)


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

    def ingest_data(self, input_data_frame: DataFrame, feature_group_arn: str, target_stores: List[str] = None,
                    use_lake_formation_credentials: bool = False):
        """
        Batch ingest data into SageMaker FeatureStore.

        :param input_data_frame (DataFrame): the DataFrame to be ingested.
        :param feature_group_arn (str): target feature group arn.
        :param target_stores (List[str]): a list of target stores which the data should be ingested to.
        :param use_lake_formation_credentials (bool): whether to use LakeFormation for offline store ingestion.
            Defaults to False. Requires PySpark 3.5 or newer; setting this to True on older
            PySpark versions will raise ``ValueError`` because the bundled Scala JAR for
            PySpark < 3.5 does not include Lake Formation support.

        :return:
        """
        if not _is_pyspark_35_or_newer() and use_lake_formation_credentials:
            raise ValueError(
                "Lake Formation credential vending requires PySpark 3.5 or newer; "
                f"detected PySpark {pyspark.__version__}"
            )
        return self._call_java("ingestDataInJava", input_data_frame, feature_group_arn, target_stores,
                               use_lake_formation_credentials)

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
