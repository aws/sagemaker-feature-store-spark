"""Shared setup for Lake Formation integration tests.

Provides helpers to configure Spark, provision an LF-governed feature group via
``sagemaker.mlops.feature_store.FeatureGroupManager``, ingest a DataFrame with
LF-vended credentials, and register cleanup. The table format (Glue/Iceberg) is
parameterized so the Hive and Iceberg tests can share all plumbing except the
verification step.
"""

import atexit
import os
import sys
import time
import unittest
from datetime import datetime, timezone
from typing import Tuple

import boto3
import feature_store_pyspark
from botocore.exceptions import ClientError
from feature_store_pyspark.FeatureStoreManager import FeatureStoreManager
from pyspark.sql import DataFrame, SparkSession
from sagemaker.core.helper.session_helper import Session as SageMakerSession
from sagemaker.core.shapes import (
    FeatureDefinition,
    OfflineStoreConfig,
    OnlineStoreConfig,
    S3StorageConfig,
)
from sagemaker.mlops.feature_store import FeatureGroupManager, LakeFormationConfig

APPENDED_COLUMNS = ["api_invocation_time", "write_time", "is_deleted"]


def build_spark() -> SparkSession:
    SPARK_HOME = "/usr/lib/spark"
    sys.path.append(f"{SPARK_HOME}/python")
    sys.path.append(f"{SPARK_HOME}/python/build")
    sys.path.append(f"{SPARK_HOME}/python/lib/py4j-src.zip")
    sys.path.append(f"{SPARK_HOME}/python/pyspark")
    sys.path.append(os.path.join(sys.path[0], "sagemaker_feature_store_pyspark.zip"))

    jars = ",".join(feature_store_pyspark.classpath_jars())
    return (
        SparkSession.builder
        .config("spark.jars", jars)
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", False)
        .getOrCreate()
    )


def provision_lf_feature_group(table_format: str, name_prefix: str) -> Tuple[str, str, str, float]:
    """Create an LF-governed feature group. Returns (fg_arn, fg_name, resolved_s3_uri, current_timestamp)."""
    sagemaker_session = SageMakerSession()
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    region = sagemaker_session.boto_session.region_name
    sagemaker_client = boto3.client("sagemaker")

    role_arn = f"arn:aws:iam::{account_id}:role/feature-store-role"
    timestamp_suffix = time.strftime("%d-%H-%M-%S", time.gmtime())
    fg_name = f"{name_prefix}-{timestamp_suffix}"
    bucket = sagemaker_session.default_bucket()
    offline_s3_uri = f"s3://{bucket}/spark-integration-test/{fg_name}"

    feature_definitions = [
        FeatureDefinition(feature_name="customer_id", feature_type="String"),
        FeatureDefinition(feature_name="event_time", feature_type="String"),
        FeatureDefinition(feature_name="age", feature_type="Integral"),
        FeatureDefinition(feature_name="total_purchases", feature_type="Integral"),
        FeatureDefinition(feature_name="avg_order_value", feature_type="Fractional"),
    ]

    FeatureGroupManager.create(
        feature_group_name=fg_name,
        record_identifier_feature_name="customer_id",
        event_time_feature_name="event_time",
        feature_definitions=feature_definitions,
        online_store_config=OnlineStoreConfig(enable_online_store=False),
        offline_store_config=OfflineStoreConfig(
            s3_storage_config=S3StorageConfig(s3_uri=offline_s3_uri),
            table_format=table_format,
        ),
        role_arn=role_arn,
        description=f"Integration test: LF-managed {table_format} feature group",
        lake_formation_config=LakeFormationConfig(
            enabled=True,
            use_service_linked_role=False,
            registration_role_arn=role_arn,
            hybrid_access_mode_enabled=False,
            acknowledge_risk=True,
        ),
        region=region,
    )

    describe = sagemaker_client.describe_feature_group(FeatureGroupName=fg_name)
    fg_arn = describe["FeatureGroupArn"]
    resolved_output_s3_uri = describe["OfflineStoreConfig"]["S3StorageConfig"]["ResolvedOutputS3Uri"]
    print(f"Feature group created: {fg_arn}")
    print(f"  Resolved S3 URI: {resolved_output_s3_uri}")

    def clean_up():
        print(f"Cleaning up feature group {fg_name}...")
        try:
            sagemaker_client.delete_feature_group(FeatureGroupName=fg_name)
            print(f"  Deleted feature group: {fg_name}")
        except ClientError as e:
            print(f"  (delete_feature_group failed: {e})")

    atexit.register(clean_up)
    return fg_arn, fg_name, resolved_output_s3_uri, time.time()


def build_test_dataframe(spark: SparkSession) -> DataFrame:
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    data = [
        ("cust-001", current_time, 32, 15, 49.99),
        ("cust-002", current_time, 45, 8, 120.50),
        ("cust-003", current_time, 28, 22, 35.75),
    ]
    return spark.createDataFrame(
        data, ["customer_id", "event_time", "age", "total_purchases", "avg_order_value"]
    )


def ingest_with_lf_credentials(fg_arn: str, df: DataFrame) -> None:
    # Using the default role (EMR instance role, a data lake admin) so LF creds are vended
    # against a principal with the required grants. Customer code would typically pass a
    # dedicated role_arn here.
    manager = FeatureStoreManager()
    print("Ingesting with use_lake_formation_credentials=True...")
    manager.ingest_data(
        input_data_frame=df,
        feature_group_arn=fg_arn,
        target_stores=["OfflineStore"],
        use_lake_formation_credentials=True,
    )
    print("Ingestion returned successfully.")


def assert_ingested(offline_store_df: DataFrame, df: DataFrame) -> None:
    tc = unittest.TestCase()
    tc.assertEqual(offline_store_df.count(), df.count())
    for row in df.collect():
        filtered = offline_store_df.filter(offline_store_df["customer_id"] == row["customer_id"])
        tc.assertEqual(filtered.count(), 1, f"Expected exactly one record for {row['customer_id']}")
        persisted = filtered.drop(*APPENDED_COLUMNS).first()
        tc.assertEqual(persisted, row)
        appended = filtered.first()
        tc.assertEqual(str(appended["is_deleted"]), "False")
