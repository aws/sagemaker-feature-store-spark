"""Lake Formation end-to-end ingestion test for Glue (Hive-partitioned) offline stores."""

from datetime import datetime, timezone

from _lf_test_common import (
    assert_ingested,
    build_spark,
    build_test_dataframe,
    ingest_with_lf_credentials,
    provision_lf_feature_group,
)

spark = build_spark()
fg_arn, fg_name, resolved_output_s3_uri, current_timestamp = provision_lf_feature_group(
    table_format="Glue", name_prefix="spark-test-lf-hive"
)
df = build_test_dataframe(spark)
ingest_with_lf_credentials(fg_arn, df)

# Glue tables use Hive-style partitioning: year=Y/month=M/day=D/hour=H/
event_time_date = datetime.fromtimestamp(current_timestamp, tz=timezone.utc)
partitioned_s3_path = "/".join(
    [
        resolved_output_s3_uri,
        f"year={event_time_date.strftime('%Y')}",
        f"month={event_time_date.strftime('%m')}",
        f"day={event_time_date.strftime('%d')}",
        f"hour={event_time_date.strftime('%H')}",
    ]
)
offline_store_df = spark.read.format("parquet").load(partitioned_s3_path)
assert_ingested(offline_store_df, df)
print("Lake Formation (Hive) integration test succeeded.")
