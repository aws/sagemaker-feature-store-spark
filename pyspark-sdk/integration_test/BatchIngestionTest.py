import subprocess
import sys
import os
import time
import atexit
import unittest

from datetime import datetime

SPARK_HOME = "/usr/lib/spark"
sys.path.append(f'{SPARK_HOME}/python')
sys.path.append(f'{SPARK_HOME}/python/build')
sys.path.append(f'{SPARK_HOME}/python/lib/py4j-src.zip')
sys.path.append(f'{SPARK_HOME}/python/pyspark')
sys.path.append(os.path.join(sys.path[0], "sagemaker_feature_store_pyspark.zip"))

import boto3
import feature_store_pyspark
from feature_store_pyspark.FeatureStoreManager import FeatureStoreManager
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import Row

# Import the required jars run the application
jars = ",".join(feature_store_pyspark.classpath_jars())
tc = unittest.TestCase()

spark = SparkSession.builder \
    .config("spark.jars", jars)\
    .config("spark.sql.sources.partitionColumnTypeInference.enabled", False)\
    .getOrCreate()

sagemaker_client = boto3.client(service_name="sagemaker")
featurestore_runtime = boto3.client(service_name="sagemaker-featurestore-runtime")
s3_client = boto3.client("s3")
caller_identity = boto3.client("sts").get_caller_identity()
account_id = caller_identity["Account"]
fraud_detection_bucket_name = "sagemaker-sample-files"
identity_file_key = "datasets/tabular/fraud_detection/synthethic_fraud_detection_SA/sampled_identity.csv"
identity_data_object = s3_client.get_object(
    Bucket=fraud_detection_bucket_name, Key=identity_file_key
)
csv_data = spark.sparkContext.parallelize(identity_data_object["Body"].read().decode("utf-8").split('\r\n'))
timestamp_suffix = time.strftime("%d-%H-%M-%S", time.gmtime())
test_feature_group_name_online_only = 'spark-test-online-only' + timestamp_suffix
test_feature_group_name_glue_table = 'spark-test-glue' + timestamp_suffix
test_feature_group_name_iceberg_table = 'spark-test-online-only' + timestamp_suffix


def clean_up(feature_group_name):
    sagemaker_client.delete_feature_group(FeatureGroupName=feature_group_name)
    print(f"Deleted feature group: {feature_group_name}")


atexit.register(clean_up, test_feature_group_name_online_only)
atexit.register(clean_up, test_feature_group_name_glue_table)
atexit.register(clean_up, test_feature_group_name_iceberg_table)

feature_store_manager = FeatureStoreManager(f"arn:aws:iam::{account_id}:role/feature-store-role")

# For testing purpose, we only get 1 record from dataset and persist it to feature store
current_time = time.time()
identity_df = spark.read.options(header='True', inferSchema='True').csv(csv_data).limit(20).cache()
identity_df = identity_df.withColumn("EventTime", lit(current_time).cast("double"))

feature_definitions = feature_store_manager.load_feature_definitions_from_schema(identity_df)


def wait_for_feature_group_creation_complete(feature_group_name):
    status = sagemaker_client.describe_feature_group(FeatureGroupName=feature_group_name).get("FeatureGroupStatus")
    while status == "Creating":
        print("Waiting for Feature Group Creation")
        time.sleep(5)
        status = sagemaker_client.describe_feature_group(FeatureGroupName=feature_group_name).get("FeatureGroupStatus")
    if status != "Created":
        raise RuntimeError(f"Failed to create feature group {feature_group_name}")

# Create a feature group with only online store enabled
response = sagemaker_client.create_feature_group(
    FeatureGroupName=test_feature_group_name_online_only,
    RecordIdentifierFeatureName='TransactionID',
    EventTimeFeatureName='EventTime',
    FeatureDefinitions=feature_definitions,
    OnlineStoreConfig={
        'EnableOnlineStore': True
    }
)

wait_for_feature_group_creation_complete(test_feature_group_name_online_only)

# Test1: Stream ingest to a feature group with only online store enabled
feature_store_manager.ingest_data(input_data_frame=identity_df, feature_group_arn=response.get("FeatureGroupArn"), target_stores=["OnlineStore"])

def verify_online_record(ingested_row: Row, record_dict: dict):
    ingested_row_dict = ingested_row.asDict()
    for key in ingested_row_dict.keys():
        ingested_value = ingested_row_dict.get(key, None)
        filterd_record_list = list(filter(lambda feature_value: feature_value["FeatureName"] == key, record_dict))
        if ingested_value is not None:
            filterd_record = filterd_record_list[0]
            tc.assertEqual(str(ingested_row_dict[key]), filterd_record["ValueAsString"])
        else:
            tc.assertEqual(len(filterd_record_list), 0)


for row in identity_df.collect():
    get_record_response = featurestore_runtime.get_record(
        FeatureGroupName=test_feature_group_name,
        RecordIdentifierValueAsString=str(row["TransactionID"]),
    )
    record = get_record_response["Record"]
    verify_online_record(row, record)


# Create a feature group with Glue table enabled
response = sagemaker_client.create_feature_group(
    FeatureGroupName=test_feature_group_name_glue_table,
    RecordIdentifierFeatureName='TransactionID',
    EventTimeFeatureName='EventTime',
    FeatureDefinitions=feature_definitions,
    OnlineStoreConfig={
        'EnableOnlineStore': True
    },
    OfflineStoreConfig={
        'S3StorageConfig': {
            'S3Uri': f's3://spark-test-bucket-{account_id}/test-offline-store'
        },
        'TableFormat': 'Glue'
    },
    RoleArn=f"arn:aws:iam::{account_id}:role/feature-store-role"
)

wait_for_feature_group_creation_complete(test_feature_group_name_glue_table)

# Test2: Batch ingest to offline store with glue table enabled
feature_store_manager.ingest_data(input_data_frame=identity_df, feature_group_arn=response.get("FeatureGroupArn"), target_stores=["OfflineStore"])

resolved_output_s3_uri = sagemaker_client.describe_feature_group(
    FeatureGroupName=test_feature_group_name_glue_table
).get("OfflineStoreConfig").get("S3StorageConfig").get("ResolvedOutputS3Uri").replace("s3", "s3a", 1)

event_time_date = datetime.fromtimestamp(current_time)

partitioned_s3_path = '/'.join([resolved_output_s3_uri,
                             f"year={event_time_date.strftime('%Y')}",
                             f"month={event_time_date.strftime('%m')}",
                             f"day={event_time_date.strftime('%d')}",
                             f"hour={event_time_date.strftime('%H')}"])

offline_store_df = spark.read.format("parquet").load(partitioned_s3_path)
appended_colums = ["api_invocation_time", "write_time", "is_deleted"]

# verify the size of input DF and offline store DF are equal
tc.assertEqual(offline_store_df.count(), identity_df.count())

def verify_appended_columns(row: Row):
    tc.assertEqual(str(row["is_deleted"]), "False")
    tc.assertEqual(datetime.fromisoformat(str(row["write_time"])),
                   datetime.fromisoformat(str(row["api_invocation_time"])))

# verify the values and appeneded columns are persisted correctly
for row in identity_df.collect():
    offline_store_filtered_df = offline_store_df.filter(
        col("TransactionID").cast("string") == str(row["TransactionID"])
    )
    tc.assertTrue(offline_store_filtered_df.count() == 1)
    tc.assertEqual(offline_store_filtered_df.drop(*appended_colums).first(), row)
    verify_appended_columns(offline_store_filtered_df.first())

# Create a feature group with Iceberg table enabled
response = sagemaker_client.create_feature_group(
    FeatureGroupName=test_feature_group_name_iceberg_table,
    RecordIdentifierFeatureName='TransactionID',
    EventTimeFeatureName='EventTime',
    FeatureDefinitions=feature_definitions,
    OnlineStoreConfig={
        'EnableOnlineStore': True
    },
    OfflineStoreConfig={
        'S3StorageConfig': {
            'S3Uri': f's3://spark-test-bucket-{account_id}/test-offline-store'
        },
        'TableFormat': 'Iceberg'
    },
    RoleArn=f"arn:aws:iam::{account_id}:role/feature-store-role"
)

wait_for_feature_group_creation_complete(test_feature_group_name_iceberg_table)

# Test3: Batch ingest to offline store with ice table enabled
feature_store_manager.ingest_data(input_data_frame=identity_df, feature_group_arn=response.get("FeatureGroupArn"), target_stores=["OfflineStore"])

resolved_output_s3_uri = sagemaker_client.describe_feature_group(
    FeatureGroupName=test_feature_group_name_glue_table
).get("OfflineStoreConfig").get("S3StorageConfig").get("ResolvedOutputS3Uri").replace("s3", "s3a", 1)

s3 = boto3.client('s3')
object_listing = s3.list_objects_v2(Bucket='pyspark-connector-dbg',
                                    Prefix='data/')


object_list = list(filter(lambda entry: f"EventTime_trunc={event_time_date.strftime('%Y-%m-%d')}" in entry['Key'], object_listing['Contents']))
tc.assertEqual(len(object_list), 1)
offline_store_df = spark.read.format("parquet").load(f"s3a://spark-test-bucket-{account_id}/object_list[0]['Key']")

# verify the values and appeneded columns are persisted correctly
for row in identity_df.collect():
    offline_store_filtered_df = offline_store_df.filter(
        col("TransactionID").cast("string") == str(row["TransactionID"])
    )
    tc.assertTrue(offline_store_filtered_df.count() == 1)
    tc.assertEqual(offline_store_filtered_df.drop(*appended_colums).first(), row)
    verify_appended_columns(offline_store_filtered_df.first())