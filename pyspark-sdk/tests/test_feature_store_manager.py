import os

from pyspark import SparkConf, SparkContext
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType

from feature_store_pyspark import classpath_jars
from feature_store_pyspark.FeatureStoreManager import FeatureStoreManager

os.environ['SPARK_CLASSPATH'] = ":".join(classpath_jars())

conf = (SparkConf().set("spark.driver.extraClassPath", os.environ['SPARK_CLASSPATH']))
sc = SparkContext(conf=conf)
spark = SparkSession(sc)


# This is a simple test to verify the ingestor can be created successfully
def test_feature_store_manager_ingest_data():
    feature_store_manager = FeatureStoreManager()
    assert feature_store_manager._wrapped_class == \
           "software.amazon.sagemaker.featurestore." \
           "sparksdk.FeatureStoreManager"

    with patch('pyspark.ml.wrapper.JavaWrapper._call_java') as java_method_invocation:
        feature_store_manager.ingest_data(None, "test-arn", True)
        # Assert call _call_java method of the wrapper with all parameters passed correctly
        java_method_invocation.assert_called_with("ingestData", None, "test-arn", True)


def test_load_feature_definitions_from_schema():
    feature_store_manager = FeatureStoreManager()
    data = [(123, 123.0, "dummy")]
    schema = StructType([
        StructField("feature-integral", LongType()),
        StructField("feature-fractional", DoubleType()),
        StructField("feature-string", StringType()),
    ])
    df = spark.createDataFrame(data, schema)
    feature_definitions = feature_store_manager.load_feature_definitions_from_schema(df)
    assert feature_definitions == [
        {
            'FeatureName': 'feature-integral',
            'FeatureType': 'Integral'
        },
        {
            'FeatureName': 'feature-fractional',
            'FeatureType': 'Fractional'
        },
        {
            'FeatureName': 'feature-string',
            'FeatureType': 'String'
        },
    ]


def test_validate_data_frame_schema():
    data = [(123, 123.0, "dummy")]
    schema = StructType([
        StructField("feature-integral", LongType()),
        StructField("feature-fractional", DoubleType()),
        StructField("feature-string", StringType()),
    ])
    df = spark.createDataFrame(data, schema)

    feature_store_manager = FeatureStoreManager()

    with patch('pyspark.ml.wrapper.JavaWrapper._call_java') as java_method_invocation:
        feature_store_manager.validate_data_frame_schema(df, "test-arn")
        # Assert call _call_java method of the wrapper with all parameters passed correctly
        java_method_invocation.assert_called_with("validateDataFrameSchema", df, "test-arn")
