SageMaker FeatureStore Spark is an open source Spark library for [Amazon SageMaker FeatureStore](https://aws.amazon.com/sagemaker/feature-store/).

With this spark connector, you can easily ingest data to FeatureGroup's online and offline store from Spark `DataFrame`.

## Supported Versions

| Component | Supported Versions |
|---|---|
| Spark | 3.1, 3.2, 3.3, 3.4, 3.5 |
| Scala | >= 2.12 |
| Python | 3.8, 3.9, 3.10, 3.11, 3.12 |
| EMR | emr-7.x and above |

> **Note:** Not all Python/PySpark combinations are supported. See the compatibility matrix below.

### Python / PySpark Compatibility Matrix

| Python \ PySpark | 3.1 | 3.2 | 3.3 | 3.4 | 3.5 |
|---|---|---|---|---|---|
| 3.8 | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3.9 | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3.10 | ❌ | ✅ | ✅ | ✅ | ✅ |
| 3.11 | ❌ | ❌ | ❌ | ✅ | ✅ |
| 3.12 | ❌ | ❌ | ❌ | ✅ | ✅ |

## Installation

### Scala

After the library is imported, you can build your application into a jar and submit the application using `spark-shell` or `spark-submit`.

The Scala SDK supports cross-building for Spark 3.1 through 3.5. Specify the target Spark version at build time:
```
sbt -DSPARK_VERSION=3.5.1 assembly
```

#### EMR

Once you import the spark library as a dependency in your application, you should be able to submit the spark job according to this EMR [documentation](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-submit-step.html).

### Python

Please make sure that the environment has PySpark and Numpy installed.

The spark library is available on [PyPi](https://pypi.org/project/sagemaker-feature-store-pyspark/).

The package bundles pre-built JARs for each supported Spark version (3.1–3.5). At runtime, the correct JAR is automatically selected based on your installed PySpark version.

If `SPARK_HOME` is set, the installer will copy the matching JAR into `$SPARK_HOME/jars`. For EMR, the path is handled automatically.

To install the library:
```
pip3 install sagemaker-feature-store-pyspark --no-binary :all:
```

#### EMR

Create a custom jar step of EMR to start the library installation.

If your EMR has single node:
```
Jar Location: command-runner.jar
Arguments: sudo -E pip3 install sagemaker-feature-store-pyspark --no-binary :all:
```
This will only install the library on the `Driver` node.

To distribute the library to all executor nodes, you can create an installation script and add a custom bootstrap while creating the EMR cluster.

For more information, take a look at [EMR bootstrap](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html).
Since bootstrap actions are executed before all EMR applications are installed, dependent jars cannot be automatically loaded to `SPARK_HOME`.
So when submitting your application, please specify dependent jars using:
```
--jars `feature-store-pyspark-dependency-jars`
```

#### SageMaker Notebook

SageMaker Notebook instances may use an older version of Spark. Install a compatible version first:

```
# Install a version of PySpark compatible with the spark library (3.1 - 3.5)
!pip3 install pyspark==3.5.1
```

## Getting Started

`FeatureStoreManager` is the interface for all Spark library operations, such as data ingestion and loading feature definitions.

### Scala

To ingest a DataFrame into FeatureStore:

```scala
import com.amazonaws.services.sagemaker.featurestore.sparksdk.FeatureStoreManager

val featureGroupArn = <your-feature-group-arn>
val featureStoreManager = new FeatureStoreManager()
featureStoreManager.ingestData(dataFrame, featureGroupArn, directOfflineStore = true)
```
If `directOfflineStore` is specified to true, the spark library will ingest data directly to OfflineStore without using FeatureStoreRuntime API which is going to cut the cost on FeatureStore WCU, the default value for this flag is false.

To load feature definitions:

```scala
val featureDefinitions = featureStoreManager.loadFeatureDefinitionsFromSchema(inputDataFrame)
```

After the feature definitions are returned, you can create feature groups using `CreateFeatureGroup` API.

### Python

To ingest a DataFrame into FeatureStore:

```python
from feature_store_pyspark.FeatureStoreManager import FeatureStoreManager

feature_group_arn = <your-feature-group-arn>
feature_store_manager = FeatureStoreManager()
feature_store_manager.ingest_data(input_data_frame=user_data_frame, feature_group_arn=feature_group_arn, direct_offline_store=True)
```
If `direct_offline_store` is specified to true, the spark library will ingest data directly to OfflineStore without using FeatureStoreRuntime API which is going to cut the cost on FeatureStore WCU, the default value for this flag is false.

To load feature definitions:

```python
feature_definitions = feature_store_manager.load_feature_definitions_from_schema(user_data_frame)
```

After the feature definitions are returned, you can create feature groups using `CreateFeatureGroup` API.

## Lake Formation Credential Vending

When your offline store's S3 location is registered with [AWS Lake Formation](https://aws.amazon.com/lake-formation/), the Spark connector can vend temporary credentials scoped to the table's data location instead of relying on the caller's IAM permissions for S3 access.

### Version Requirements

Lake Formation credential vending requires Spark/PySpark 3.5 or newer. On older Spark versions, the `use_lake_formation_credentials` / `useLakeFormationCredentials` parameter is not available (Scala) or will raise `ValueError` if set to `True` (Python).

### Usage

#### Scala

```scala
val featureStoreManager = new FeatureStoreManager()
featureStoreManager.ingestData(
  dataFrame,
  featureGroupArn,
  directOfflineStore = true,
  useLakeFormationCredentials = true
)
```

#### Python

```python
feature_store_manager = FeatureStoreManager()
feature_store_manager.ingest_data(
    input_data_frame=df,
    feature_group_arn=feature_group_arn,
    direct_offline_store=True,
    use_lake_formation_credentials=True
)
```

### Prerequisites

1. The offline store S3 location must be [registered with Lake Formation](https://docs.aws.amazon.com/lake-formation/latest/dg/register-data-lake.html).
   You can use the [SageMaker Python SDK](https://github.com/aws/sagemaker-python-sdk) to enable Lake Formation governance of a Feature Group's offline store.
2. The IAM role running the Spark job (the "ingestion role") must have:
   - `lakeformation:GetDataAccess` and `lakeformation:GetTemporaryGlueTableCredentials`.
   - `glue:GetTable`, `glue:GetDatabase`, and `glue:GetPartitions` on the feature group's Glue catalog.
   - `sagemaker:DescribeFeatureGroup` on the feature group.
3. The Lake Formation table must have `SELECT`, `INSERT`, `DELETE`, and `DESCRIBE` granted on the **Table** resource to the ingestion role. `GetTemporaryGlueTableCredentials` validates permissions at the Table level; a column-only `SELECT` grant is not sufficient and returns `Insufficient Lake Formation permission(s): SUPER privileges required on the table`.
4. The Lake Formation account-level settings must allow third-party data access (required so Lake Formation will vend temporary credentials to the Spark connector):

   ```bash
   aws lakeformation put-data-lake-settings \
     --region <region> \
     --data-lake-settings '{
       "DataLakeAdmins": [...],
       "AllowExternalDataFiltering": true,
       "AllowFullTableExternalDataAccess": true,
       "ExternalDataFilteringAllowList": [
         {"DataLakePrincipalIdentifier": "<account-id>"}
       ],
       "CreateDatabaseDefaultPermissions": [],
       "CreateTableDefaultPermissions": []
     }'
   ```

   - `AllowExternalDataFiltering: true` permits Amazon EMR (and other third-party engines) to access data in S3 locations registered with Lake Formation.
   - `AllowFullTableExternalDataAccess: true` permits third-party engines like Spark to receive data access credentials without session tags, which `GetTemporaryGlueTableCredentials` requires. Without it, credential vending fails with `Not authorized to call GetTemporaryCredentialsForTableV2`.
   - `ExternalDataFilteringAllowList` must contain the account(s) whose principals will call the connector.

   See the AWS docs on [external data filtering](https://docs.aws.amazon.com/lake-formation/latest/dg/third-party-services.html) for details.
5. An S3A magic committer implementation must be available. The connector enables the S3A magic committer to let Parquet writes stay within the Lake Formation-scoped S3 prefix.

   - On **EMR 6.15+/7.x**: no action required. EMR ships its proprietary `SQLEmrOptimizedCommitProtocol` which the connector auto-detects and uses.
   - On **AWS Glue**, **SageMaker Notebook**, **standalone PySpark**, or any other non-EMR runtime: add the open-source `spark-hadoop-cloud` module at submit time, for example:

     ```bash
     spark-submit \
       --packages org.apache.spark:spark-hadoop-cloud_2.12:<spark-version> \
       your_job.py
     ```

     Or when building the `SparkSession` programmatically:

     ```python
     SparkSession.builder \
         .config("spark.jars.packages", "org.apache.spark:spark-hadoop-cloud_2.12:3.5.1") \
         .getOrCreate()
     ```

   If neither is available on the classpath, the connector fails fast with a clear error describing how to add it.

### Cross-Account Access

The connector supports cross-account credential vending. This allows a Spark job running in Account B to ingest data into a feature group owned by Account A, as long as the appropriate permissions are in place.

The feature group ARN contains the owning account's ID, which the connector uses to build the Glue table ARN for credential vending. No additional connector configuration is needed for cross-account access.

#### Cross-Account Prerequisites

1. **Lake Formation grant** (run as Account A, the data owner):
   ```bash
   aws lakeformation grant-permissions \
     --principal DataLakePrincipalIdentifier=<account-b-id> \
     --resource '{"Table":{"DatabaseName":"<database>","Name":"<table>","CatalogId":"<account-a-id>"}}' \
     --permissions SELECT DESCRIBE INSERT \
     --region <region>
   ```

2. **Glue cross-account access** (run as Account A):
   ```bash
   aws glue put-resource-policy \
     --enable-hybrid TRUE \
     --policy-in-json '{
       "Version": "2012-10-17",
       "Statement": [{
         "Effect": "Allow",
         "Principal": {"AWS": "<account-b-id>"},
         "Action": ["glue:GetTable", "glue:GetDatabase", "glue:GetTables"],
         "Resource": [
           "arn:aws:glue:<region>:<account-a-id>:catalog",
           "arn:aws:glue:<region>:<account-a-id>:database/<database>",
           "arn:aws:glue:<region>:<account-a-id>:table/<database>/*"
         ]
       }]
     }' \
     --region <region>
   ```
   > **Note:** The `--enable-hybrid TRUE` flag is required if Account A uses Lake Formation hybrid access mode.

3. **Account B IAM permissions**: The role running the Spark job needs `lakeformation:GetDataAccess` and `sagemaker:DescribeFeatureGroup` on Account A's feature group.

### Troubleshooting

If credential vending fails, check the following:

1. **`AccessDeniedException: not authorized to perform lakeformation:GetDataAccess`**
   - The Lake Formation table grant is missing. Run the `grant-permissions` command above from the data owner's account.
   - Verify the grant exists:
     ```bash
     aws lakeformation list-permissions \
       --resource '{"Table":{"DatabaseName":"<database>","Name":"<table>","CatalogId":"<account-a-id>"}}' \
       --region <region>
     ```

2. **`AccessDeniedException: Insufficient Glue permissions to access table`**
   - The Glue resource policy is missing or does not include the caller's account. Run the `put-resource-policy` command above.
   - Verify the policy:
     ```bash
     aws glue get-resource-policy --region <region>
     ```

3. **`AccessDeniedException: not authorized to perform glue:GetTable`**
   - Same as above — the Glue resource policy needs to allow `glue:GetTable` for the caller's account on the data owner's catalog.

4. **Credentials vended but S3 access denied**
   - The S3 location may not be registered with Lake Formation. Verify:
     ```bash
     aws lakeformation list-resources --region <region>
     ```
     Ensure the offline store S3 path appears in the registered locations.
   - The Lake Formation data access role may not have S3 permissions on the actual bucket/prefix.

5. **General debugging**
   - Confirm caller identity: `aws sts get-caller-identity`
   - Confirm the feature group's offline store config:
     ```bash
     aws sagemaker describe-feature-group \
       --feature-group-name <feature-group-arn> \
       --region <region> \
       --query 'OfflineStoreConfig.DataCatalogConfig'
     ```
   - Test credential vending directly:
     ```bash
     aws lakeformation get-temporary-glue-table-credentials \
       --table-arn "arn:aws:glue:<region>:<account-a-id>:table/<database>/<table>" \
       --permissions SELECT INSERT DESCRIBE \
       --duration-seconds 900 \
       --region <region>
     ```

## Development

### New Features

This library is built in Scala, and Python methods are actually calling Scala via JVM through `wrapper.py`. To add more features, please make sure you finish the implementation in Scala first and perhaps you need to update the wrapper so that functionality of Scala and Python are in sync.

Note: Spark 3.5 introduced a breaking change in the `ExpressionEncoder` API. The library handles this with version-specific source directories (`scala-spark-3.5` and `scala-spark-3.1-3.4`) that are selected at build time.

### Test Coverage

Both Scala and Python versions have unit tests covered. In addition to that, we have integration tests for the Python version which verify there are no regressions in terms of functionality.

#### Scala Package Build

We use `scalafmt` to auto format the code. Please run `sbt scalafmtAll` to format your code.

To get the test coverage report and format check result, run `sbt jacoco scalafmtCheckAll`.

To build for a specific Spark version:
```
sbt -DSPARK_VERSION=3.5.1 assembly
```

#### Python Package Build

We are using `tox` for test purposes. The test matrix covers Python 3.8–3.12 against PySpark 3.2–3.5. You can check the build by running `tox`. To configure or figure out the commands we run, please check `tox.ini`.

#### Integration Test

The test execution script and tests are included in `pyspark-sdk/integration_test`. The runner submits each test as a separate EMR step:

- `BatchIngestionTest.py` — end-to-end online/offline ingestion against Glue and Iceberg tables.
- `LakeFormationHiveIngestionTest.py` — end-to-end ingestion with `use_lake_formation_credentials=True` against a Glue (Hive-partitioned) offline store.
- `LakeFormationIcebergIngestionTest.py` — end-to-end ingestion with `use_lake_formation_credentials=True` against an Iceberg offline store.

To run:

1. Fetch the credential from our spark test account first.
2. Run the test execution script `run-spark-integration-test`

Integration tests run on Python 3.10 + PySpark 3.5.1.

#### GitHub Repository Automated Testing

The GitHub repository uses GitHub Actions for CI. The workflow runs unit tests across the full Python/PySpark version matrix and integration tests on Python 3.10 + PySpark 3.5.1. See `.github/workflows/integration-tests.yml` for details.

## More Reference

[Spark Application on EMR](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-application.html)

[Add Spark EMR Steps](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-submit-step.html)

## License

This project is licensed under the Apache-2.0 License.
