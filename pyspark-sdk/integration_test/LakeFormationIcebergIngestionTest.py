"""Lake Formation end-to-end ingestion test for Iceberg offline stores.

Reads the Iceberg table through the Glue catalog via pyiceberg to verify records
landed, rather than walking the data prefix on S3 (Iceberg does not use
Hive-style partitioning on S3 keys).
"""

import boto3
from pyiceberg.catalog import load_catalog

from _lf_test_common import (
    assert_ingested,
    build_spark,
    build_test_dataframe,
    ingest_with_lf_credentials,
    provision_lf_feature_group,
)

spark = build_spark()
fg_arn, fg_name, resolved_output_s3_uri, _current_timestamp = provision_lf_feature_group(
    table_format="Iceberg", name_prefix="spark-test-lf-iceberg"
)
df = build_test_dataframe(spark)
ingest_with_lf_credentials(fg_arn, df)

# Resolve the Glue database/table the feature group wrote to.
sagemaker_client = boto3.client("sagemaker")
describe = sagemaker_client.describe_feature_group(FeatureGroupName=fg_name)
data_catalog = describe["OfflineStoreConfig"]["DataCatalogConfig"]
database, table = data_catalog["Database"], data_catalog["TableName"]

account_id = boto3.client("sts").get_caller_identity()["Account"]
region = boto3.session.Session().region_name or "us-west-2"

catalog = load_catalog(
    "default",
    **{
        "type": "glue",
        "glue.id": account_id,
        "glue.region": region,
        "s3.region": region,
    },
)
iceberg_df = spark.createDataFrame(
    catalog.load_table((database, table)).scan().to_pandas()
)
assert_ingested(iceberg_df, df)
print("Lake Formation (Iceberg) integration test succeeded.")
