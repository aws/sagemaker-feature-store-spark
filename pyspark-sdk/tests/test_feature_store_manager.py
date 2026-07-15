import os

import pytest
from pyspark import SparkConf, SparkContext
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType

from feature_store_pyspark import classpath_jars
from feature_store_pyspark import FeatureStoreManager as fsm_module
from feature_store_pyspark.FeatureStoreManager import FeatureStoreManager

os.environ['SPARK_CLASSPATH'] = ":".join(classpath_jars())

conf = (SparkConf().set("spark.driver.extraClassPath", os.environ['SPARK_CLASSPATH']))
sc = SparkContext(conf=conf)
spark = SparkSession(sc)


# This is a simple test to verify the ingestor can be created successfully
def test_feature_store_manager_methods():
    feature_store_manager = FeatureStoreManager()
    assert feature_store_manager._wrapped_class == \
           "software.amazon.sagemaker.featurestore." \
           "sparksdk.FeatureStoreManager"

    with patch('pyspark.ml.wrapper.JavaWrapper._call_java') as java_method_invocation:
        # Default use_lake_formation_credentials=False, use_batch_write_record=False.
        feature_store_manager.ingest_data(None, "test-arn", ["OnlineStore"])
        java_method_invocation.assert_called_with("ingestDataInJava", None, "test-arn", ["OnlineStore"], False, False)

        feature_store_manager.ingest_data(None, "test-arn", ["OfflineStore"], use_lake_formation_credentials=False)
        java_method_invocation.assert_called_with("ingestDataInJava", None, "test-arn", ["OfflineStore"], False, False)

        feature_store_manager.get_failed_stream_ingestion_data_frame()
        java_method_invocation.assert_called_with("getFailedStreamIngestionDataFrame")


def test_ingest_data_rejects_lf_on_old_pyspark():
    feature_store_manager = FeatureStoreManager()
    with patch.object(fsm_module, "_is_pyspark_35_or_newer", return_value=False), \
         patch.object(fsm_module, "pyspark") as mock_pyspark, \
         patch('pyspark.ml.wrapper.JavaWrapper._call_java') as java_method_invocation:
        mock_pyspark.__version__ = "3.3.0"
        with pytest.raises(ValueError, match="requires PySpark 3.5 or newer.*3\\.3\\.0"):
            feature_store_manager.ingest_data(None, "test-arn", ["OfflineStore"],
                                              use_lake_formation_credentials=True)
        java_method_invocation.assert_not_called()


def test_ingest_data_lf_on_new_pyspark_calls_4_arg():
    feature_store_manager = FeatureStoreManager()
    with patch.object(fsm_module, "_is_pyspark_35_or_newer", return_value=True), \
         patch('pyspark.ml.wrapper.JavaWrapper._call_java') as java_method_invocation:
        feature_store_manager.ingest_data(None, "test-arn", ["OfflineStore"],
                                          use_lake_formation_credentials=True)
        java_method_invocation.assert_called_with("ingestDataInJava", None, "test-arn", ["OfflineStore"], True, False)


def test_ingest_data_no_lf_on_old_pyspark_calls_4_arg():
    feature_store_manager = FeatureStoreManager()
    with patch.object(fsm_module, "_is_pyspark_35_or_newer", return_value=False), \
         patch('pyspark.ml.wrapper.JavaWrapper._call_java') as java_method_invocation:
        # Even on old PySpark the Scala JAR exposes the 5-arg method; the wrapper forwards False for both flags.
        feature_store_manager.ingest_data(None, "test-arn", ["OfflineStore"])
        java_method_invocation.assert_called_with("ingestDataInJava", None, "test-arn", ["OfflineStore"], False, False)


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


# ============================================================================
# BatchWriteRecord (use_batch_write_record flag) tests
# ============================================================================

def test_ingest_data_batch_write_record_default_false():
    """use_batch_write_record defaults to False, passes False to Java."""
    feature_store_manager = FeatureStoreManager()
    with patch('pyspark.ml.wrapper.JavaWrapper._call_java') as java_method_invocation:
        feature_store_manager.ingest_data(None, "test-arn", ["OnlineStore"])
        java_method_invocation.assert_called_with(
            "ingestDataInJava", None, "test-arn", ["OnlineStore"], False, False
        )


def test_ingest_data_batch_write_record_true():
    """use_batch_write_record=True passes True to Java."""
    feature_store_manager = FeatureStoreManager()
    with patch('pyspark.ml.wrapper.JavaWrapper._call_java') as java_method_invocation:
        feature_store_manager.ingest_data(None, "test-arn", None, use_batch_write_record=True)
        java_method_invocation.assert_called_with(
            "ingestDataInJava", None, "test-arn", None, False, True
        )


def test_ingest_data_batch_write_record_false_explicit():
    """use_batch_write_record=False explicitly passes False to Java."""
    feature_store_manager = FeatureStoreManager()
    with patch('pyspark.ml.wrapper.JavaWrapper._call_java') as java_method_invocation:
        feature_store_manager.ingest_data(None, "test-arn", None, use_batch_write_record=False)
        java_method_invocation.assert_called_with(
            "ingestDataInJava", None, "test-arn", None, False, False
        )


def test_ingest_data_batch_write_with_lf_and_bwr():
    """Both use_lake_formation_credentials=True and use_batch_write_record=True."""
    feature_store_manager = FeatureStoreManager()
    with patch.object(fsm_module, "_is_pyspark_35_or_newer", return_value=True), \
         patch('pyspark.ml.wrapper.JavaWrapper._call_java') as java_method_invocation:
        feature_store_manager.ingest_data(
            None, "test-arn", ["OfflineStore"],
            use_lake_formation_credentials=True,
            use_batch_write_record=True,
        )
        java_method_invocation.assert_called_with(
            "ingestDataInJava", None, "test-arn", ["OfflineStore"], True, True
        )


def test_ingest_data_batch_write_with_target_stores():
    """target_stores + use_batch_write_record=True."""
    feature_store_manager = FeatureStoreManager()
    with patch('pyspark.ml.wrapper.JavaWrapper._call_java') as java_method_invocation:
        feature_store_manager.ingest_data(
            None, "test-arn", ["OnlineStore"],
            use_batch_write_record=True,
        )
        java_method_invocation.assert_called_with(
            "ingestDataInJava", None, "test-arn", ["OnlineStore"], False, True
        )


# ============================================================================
# ListRecords tests
# ============================================================================

def test_list_records_basic():
    """list_records calls Java with correct args and returns dict."""
    feature_store_manager = FeatureStoreManager()
    with patch('pyspark.ml.wrapper.JavaWrapper._call_java') as java_method_invocation:
        mock_result = {"RecordIdentifiers": ["id1", "id2"], "NextToken": None}
        java_method_invocation.return_value = mock_result
        result = feature_store_manager.list_records("test-arn", max_results=10)
        java_method_invocation.assert_called_with(
            "listRecords", "test-arn", 10, None, False
        )
        assert result["RecordIdentifiers"] == ["id1", "id2"]
        assert result["NextToken"] is None


def test_list_records_with_next_token():
    """list_records passes next_token correctly."""
    feature_store_manager = FeatureStoreManager()
    with patch('pyspark.ml.wrapper.JavaWrapper._call_java') as java_method_invocation:
        mock_result = {"RecordIdentifiers": ["id3"], "NextToken": "token2"}
        java_method_invocation.return_value = mock_result
        result = feature_store_manager.list_records("test-arn", next_token="token1")
        java_method_invocation.assert_called_with(
            "listRecords", "test-arn", None, "token1", False
        )
        assert result["NextToken"] == "token2"


def test_list_records_with_soft_deleted():
    """list_records passes include_soft_deleted_records correctly."""
    feature_store_manager = FeatureStoreManager()
    with patch('pyspark.ml.wrapper.JavaWrapper._call_java') as java_method_invocation:
        mock_result = {"RecordIdentifiers": ["id1"], "NextToken": None}
        java_method_invocation.return_value = mock_result
        result = feature_store_manager.list_records(
            "test-arn", include_soft_deleted_records=True
        )
        java_method_invocation.assert_called_with(
            "listRecords", "test-arn", None, None, True
        )


def test_list_records_all_params():
    """list_records with all params."""
    feature_store_manager = FeatureStoreManager()
    with patch('pyspark.ml.wrapper.JavaWrapper._call_java') as java_method_invocation:
        mock_result = {"RecordIdentifiers": [], "NextToken": None}
        java_method_invocation.return_value = mock_result
        result = feature_store_manager.list_records(
            "test-arn", max_results=5, next_token="abc", include_soft_deleted_records=True
        )
        java_method_invocation.assert_called_with(
            "listRecords", "test-arn", 5, "abc", True
        )
