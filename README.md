# <img alt="SageMaker" src="icon/sagemaker-banner.png" />

SageMaker FeatureStore Spark is an open source Spark library for [Amazon SageMaker FeatureStore](https://aws.amazon.com/sagemaker/feature-store/).

With this spark library, you can easily ingest data to FeatureGroup's online and offline store from Spark `DataFrame`. Besides the spark library also provides method to load feature definitions.

## Installation

### Scala

The library is compatible with Scala >= 2.12, and Spark >= 3.0.0. 
If your application is on EMR, pleas use emr-6.x.

TODO: Add instructions here how to install the library from Maven.

After the library is imported, you can build your application into a jar and submit the application using `spark-shell` or `spark-submit`.

And if you prefer to checkout the library locally, you can specify the library as dependency in `--jars` flag.

### Python

The library is compatible with Python >= 3.6, and PySpark >= 3.0.0.
If your application is on EMR, pleas use emr-6.x.

Please also make sure that the environment has PySpark and Numpy installed.

The spark library is available on [PyPi](https://pypi.org/project/sagemaker-feature-store-pyspark/)
Before installation, it is recommended to set `SPARK_HOME` environment variable to the path where your Spark is installed, because during installation the library will automatically copy some depedent jars to `SPARK_HOME`.
For EMR, the library installation will handle the path automatically, so there is no need to specify `SPARK_HOME` if you're installing on EMR.

To install the library:
```
sudo -E pip3 install sagemaker-feature-store-pyspark --no-binary :all:
```

#### EMR

Create a custom jar step of EMR to start the library installation

If your EMR has single node:
```
Jar Location: command-runner.jar
Arguments: sudo -E pip3 install sagemaker-feature-store-pyspark —no-binary :all:
```
This will only install the library on `Driver` node.

To distribute the library to all executor nodes, you can create a installation script and add a custom bootstrap while creating EMR cluster.

For more information, pleas take a look at [EMR bootstramp](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html).
Since bootstrap action is executed before all EMR applications are installed, so dependent jars cannot be automatically loaded to `SPARK_HOME`.
So when submitting your application, please specify dependent jars using:
```
--jars `feature-store-pyspark-dependency-jars`
```

#### SageMaker Notebook

Since SageMaker Notebook instances are using older version of Spark library which is not compatible with the spark version of FeatureStore Spark library. The Spark on SageMaker Notebook instance has to be uninstalled first and reinstall with a newer version.

So, add a cell like this in your notebook:
```
# Install a newer versiion of Spark which is compatible with spark library
!pip3 install pyspark==3.1.1
```

After finish executing the notebook, you can restore the original version which is Spark-2.4.0.

## Getting Started

`FeatureStoreManager` is the interface for all Spark library operations, such as data ingestion and loading feature definitions.

### Scala

To ingest a DataFrame into FeatureStore:

```
import com.amazonaws.services.sagemaker.featurestore.sparksdk.FeatureStoreManager

val featureGroupArn = <your-feature-group-arn>
val featureStoreManager = new FeatureStoreManager()
featureStoreManager.ingestData(dataFrame, featureGroupArn, directOfflineStore = true)
```
If `directOfflineStore` is specified to true, the spark library will ingest data directly to OfflineStore without using FeatureStoreRuntime API which is going to cut the cost on FeatureStore WCU, the default value for this flag is false.

To load feature definitions:

```
val featureDefinitions = featureStoreManager.loadFeatureDefinitionsFromSchema(inputDataFrame)
```

After the feature definitions are retured, you can create feature groups using `CreateFeatureGroup` API.

### Python

To ingest a DataFrame into FeatureStore:

```
from feature_store_pyspark.FeatureStoreManager import FeatureStoreManager

feature_group_arn = <your-feature-group-arn>
feature_store_manager = FeatureStoreManager()
feature_store_manager.ingest_data(input_data_frame=user_data_frame, feature_group_arn=feature_group_arn, direct_offline_store=True)
```
If `direct_offline_store` is specified to true, the spark library will ingest data directly to OfflineStore without using FeatureStoreRuntime API which is going to cut the cost on FeatureStore WCU, the default value for this flag is false.

To load feature definitions:

```
feature_definitions = feature_store_manager.load_feature_definitions_from_schema(user_data_frame)
```

After the feature definitions are retured, you can create feature groups using `CreateFeatureGroup` API.

## More Reference

[Spark Application on EMR](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-application.html)

[Add Spark EMR Steps](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-submit-step.html)

## License

This project is licensed under the Apache-2.0 License.






