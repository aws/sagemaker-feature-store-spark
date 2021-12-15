SageMaker FeatureStore Spark is a connector library for [Amazon SageMaker FeatureStore](https://aws.amazon.com/sagemaker/feature-store/).

With this spark connector, you can easily ingest data to FeatureGroup's online and offline store from Spark `DataFrame`. Also, this connector contains the functionality to automatically load feature definitions to help with creating feature groups.

## Getting Started

### Requires

PySpark >= 3.0.0

Python >= 3.6

If you’re using EMR

EMR release version > 6.x

### Installation

Before installation, it is recommended to set `SPARK_HOME` environment variable to the path where your Spark is installed, because during installation the library will automatically copy some depedent jars to `SPARK_HOME`.

```
pip3 install sagemaker-feature-store-pyspark --no-binary :all:
```
To learn more info about installation, please also enable verbose mode by appending `--verbose` at the end.

#### SageMaker Notebook

If you want to try out the connector on SageMaker notebook, extra steps of installation are needed.

Since SageMaker Notebook instances are using older version of Spark which is not compatible with feature store spark connector. We have to override with newer Spark version on SageMaker Notebook instance.

Add a cell like this：

```
import os

original_spark_version = "2.4.0"
os.environ['SPARK_HOME'] = '/home/ec2-user/anaconda3/envs/python3/lib/python3.6/site-packages/pyspark'

# Install a newer versiion of Spark which is compatible with spark library
!pip3 install pyspark==3.1.2
!pip3 install sagemaker-feature-store-pyspark --no-binary :all:
```

#### EMR

If you are installing the spark connector on EMR, please set `SPARK_HOME` as `/usr/lib/spark` on master node.

Note: If you want to install the dependent jars automatically to `SPARK_HOME`, please do not use EMR’s bootstrap. Simply use custom steps or ssh to the instance directly to finish the installation.

### Using Connector in Development Environment

After installing the spark connector, in the Python interpretor:

```
from pyspark.sql import SparkSession
from feature_store_pyspark.FeatureStoreManager import FeatureStoreManager
import feature_store_pyspark

extra_jars = ",".join(feature_store_pyspark.classpath_jars())
spark = SparkSession.builder \
                    .config("spark.jars", jars) \
                    .getOrCreate()

# Construct test DataFrame
columns = ["RecordIdentifier", "EventTime"]
data = [("1","2021-03-02T12:20:12Z"), ("2", "2021-03-02T12:20:13Z"), ("3", "2021-03-02T12:20:14Z")]

df = spark.createDataFrame(data).toDF(*columns)
feature_store_manager= FeatureStoreManager()
 
# Load the feature definitions from input schema. The feature definitions can be used to create a feature group
feature_definitions = feature_store_manager.load_feature_definitions_from_schema(df)

feature_group_arn = "YOUR_FEATURE_GROUP_ARN"

# Ingest by default
feature_store_manager.ingest_data(input_data_frame=df, feature_group_arn=feature_group_arn)

# Offline store direct ingestion, flip the flag of direct_offline_store
feature_store_manager.ingest_data(input_data_frame=df, feature_group_arn=feature_group_arn, direct_offline_store=true)
```

### Submitting Spark Job

When submitting the spark job, please make sure the dependent jars are added to the classpath.

If you did not specify the `SPARK_HOME` during installation, `feature-store-pyspark-dependency-jars` is a python script installed by spark library to automatically fetch the paths to all jars needed for you.

```
spark-submit --jars `feature-store-pyspark-dependency-jars` PATH_TO_YOUR_SPARK_PYTHON_SCRIPT
```

If you are running application on EMR, it’s recommended to run the application in client mode, so that you do not need to distribute the dependent jars to other task nodes. Add one more step in EMR cluster with Spark argument like this:
```
spark-submit --deploy-mode client --master yarn PATH_TO_YOUR_SPARK_PYTHON_SCRIPT
```