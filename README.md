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

The test execution script and test itself are included in `pyspark-sdk/integration_test`, to run the test:

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
