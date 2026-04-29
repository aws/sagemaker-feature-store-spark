# SageMaker FeatureStore PySpark

SageMaker FeatureStore Spark is an open source Spark library for
[Amazon SageMaker FeatureStore](https://aws.amazon.com/sagemaker/feature-store/).
With this connector, you can easily ingest data to FeatureGroup's online and
offline store from Spark `DataFrame`. This package provides the Python (PySpark)
interface.

For full documentation including Scala usage, cross-account Lake Formation
access, and troubleshooting, see the
[GitHub repository](https://github.com/aws/sagemaker-feature-store-spark).

## Supported Versions

| Component | Supported Versions |
|---|---|
| Spark | 3.1, 3.2, 3.3, 3.4, 3.5 |
| Python | 3.8, 3.9, 3.10, 3.11, 3.12 |
| EMR | emr-7.x and above |

> **Note:** Not all Python/PySpark combinations are supported. See the
> compatibility matrix below.

### Python / PySpark Compatibility Matrix

| Python \ PySpark | 3.1 | 3.2 | 3.3 | 3.4 | 3.5 |
|---|---|---|---|---|---|
| 3.8 | Yes | Yes | Yes | Yes | Yes |
| 3.9 | Yes | Yes | Yes | Yes | Yes |
| 3.10 | No | Yes | Yes | Yes | Yes |
| 3.11 | No | No | No | Yes | Yes |
| 3.12 | No | No | No | Yes | Yes |

> **Note:** PySpark versions older than 3.5 are in maintenance mode and will not
> receive new features. New functionality is only added for PySpark 3.5+.

## Installation

**Prerequisites:** PySpark and NumPy must be installed in your environment.

The package is available on [PyPI](https://pypi.org/project/sagemaker-feature-store-pyspark/).
It bundles pre-built JARs for each supported Spark version (3.1-3.5). At
runtime, the correct JAR is automatically selected based on your installed
PySpark version.

If `SPARK_HOME` is set, the installer copies the matching JAR into
`$SPARK_HOME/jars`. For EMR, the path is handled automatically.

```
pip3 install sagemaker-feature-store-pyspark --no-binary :all:
```

### EMR

Create a custom jar step to install the library:

- **Jar Location:** `command-runner.jar`
- **Arguments:** `sudo -E pip3 install sagemaker-feature-store-pyspark --no-binary :all:`

This installs the library on the Driver node only. To distribute to all executor
nodes, create an installation script and add a custom
[bootstrap action](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html)
when creating the EMR cluster.

Since bootstrap actions run before EMR applications are installed, dependent JARs
cannot be automatically loaded to `SPARK_HOME`. When submitting your application,
specify dependent JARs using:

```
--jars `feature-store-pyspark-dependency-jars`
```

### SageMaker Notebook

SageMaker Notebook instances may use an older version of Spark. Install a
compatible version first:

```
# Install a version of PySpark compatible with the library (3.1 - 3.5)
!pip3 install pyspark==3.5.1
```

## Getting Started

`FeatureStoreManager` is the main interface for all library operations, including
data ingestion and loading feature definitions.

### Ingest Data

```python
from feature_store_pyspark.FeatureStoreManager import FeatureStoreManager

feature_group_arn = "arn:aws:sagemaker:...:feature-group/your-feature-group"
feature_store_manager = FeatureStoreManager()
feature_store_manager.ingest_data(
    input_data_frame=df,
    feature_group_arn=feature_group_arn,
    target_stores=["OfflineStore"]
)
```

If `target_stores` is set to `["OfflineStore"]`, data is ingested directly to
the offline store without using the FeatureStore Runtime API, reducing WCU costs.
The default is `None` (ingests to both online and offline stores).

### Load Feature Definitions

```python
feature_definitions = feature_store_manager.load_feature_definitions_from_schema(df)
```

Returns feature definitions that can be used with the
[CreateFeatureGroup](https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreateFeatureGroup.html)
API.

### Retrieve Failed Ingestion Records

```python
failed_df = feature_store_manager.get_failed_stream_ingestion_data_frame()
```

Returns a DataFrame containing records that failed during `ingest_data()`.

## Lake Formation Support

When your offline store's S3 location is registered with
[AWS Lake Formation](https://aws.amazon.com/lake-formation/), enable the
`use_lake_formation_credentials` parameter (requires PySpark 3.5+):

```python
feature_store_manager.ingest_data(
    input_data_frame=df,
    feature_group_arn=feature_group_arn,
    target_stores=["OfflineStore"],
    use_lake_formation_credentials=True
)
```

For prerequisites, cross-account access, and troubleshooting, see the
[main repository README](https://github.com/aws/sagemaker-feature-store-spark).

## License

This project is licensed under the Apache-2.0 License.
