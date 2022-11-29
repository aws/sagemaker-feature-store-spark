package software.amazon.sagemaker.featurestore.sparksdk.helpers

import org.scalatestplus.testng.TestNGSuite
import org.testng.annotations.{DataProvider, Test}
import org.apache.spark.sql.SparkSession
import org.testng.Assert.assertEquals

class SparkSessionInitializerTest extends TestNGSuite {

  private final val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("TestProgram")
    .master("local")
    .getOrCreate()

  @Test(dataProvider = "initializeSparkSessionForOfflineStoreDataProvider")
  def initializeSparkSessionForOfflineStoreTest(
      offlineStoreKmsKeyId: String,
      assumeRoleArn: String,
      region: String,
      expectedS3Endpoint: String
  ): Unit = {
    SparkSessionInitializer.initializeSparkSessionForOfflineStore(
      sparkSession,
      offlineStoreKmsKeyId,
      assumeRoleArn,
      region
    )

    if (assumeRoleArn == null) {
      assertEquals(
        sparkSession.sparkContext.hadoopConfiguration
          .get("fs.s3a.aws.credentials.provider"),
        "com.amazonaws.auth.ContainerCredentialsProvider," +
          "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
      )
    } else {
      assertEquals(
        sparkSession.sparkContext.hadoopConfiguration
          .get("fs.s3a.aws.credentials.provider"),
        "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"
      )

      assertEquals(
        sparkSession.sparkContext.hadoopConfiguration
          .get("fs.s3a.assumed.role.credentials.provider"),
        "com.amazonaws.auth.ContainerCredentialsProvider," +
          "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
      )

      assertEquals(
        sparkSession.sparkContext.hadoopConfiguration
          .get("fs.s3a.assumed.role.arn"),
        assumeRoleArn
      )
    }

    val hadoopConfiguration = sparkSession.sparkContext.hadoopConfiguration
    assertEquals(
      hadoopConfiguration.get(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs"
      ),
      "false"
    )
    assertEquals(
      hadoopConfiguration.get("parquet.summary.metadata.level"),
      "NONE"
    )
    assertEquals(
      hadoopConfiguration.get("fs.s3a.server-side-encryption-algorithm"),
      "SSE-KMS"
    )

    if (offlineStoreKmsKeyId != null) {
      assertEquals(
        hadoopConfiguration.get("fs.s3a.server-side-encryption.key"),
        offlineStoreKmsKeyId
      )
    }

    if (region.startsWith("cn-")) {
      assertEquals(
        hadoopConfiguration.get("fs.s3a.endpoint"),
        expectedS3Endpoint
      )
    } else {
      assertEquals(
        hadoopConfiguration.get("fs.s3a.endpoint"),
        expectedS3Endpoint
      )
    }
  }

  @Test(dataProvider = "initializeSparkSessionForIcebergTableDataProvider")
  def initializeSparkSessionForIcebergTableTest(
      offlineStoreEncryptionKmsKeyId: String,
      resolvedOutputS3Uri: String,
      dataCatalogName: String,
      assumeRoleArn: String,
      region: String
  ): Unit = {

    SparkSessionInitializer.initializeSparkSessionForIcebergTable(
      sparkSession,
      offlineStoreEncryptionKmsKeyId,
      resolvedOutputS3Uri,
      dataCatalogName,
      assumeRoleArn,
      region
    )

    if (offlineStoreEncryptionKmsKeyId != null) {
      assertEquals(
        sparkSession.conf.get(f"spark.sql.catalog.$dataCatalogName.s3.sse.key"),
        offlineStoreEncryptionKmsKeyId
      )
    }

    if (assumeRoleArn != null) {
      assertEquals(sparkSession.conf.get(f"spark.sql.catalog.$dataCatalogName.client.assume-role.arn"), assumeRoleArn)
      assertEquals(sparkSession.conf.get(f"spark.sql.catalog.$dataCatalogName.client.assume-role.region"), region)
      assertEquals(
        sparkSession.conf.get(f"spark.sql.catalog.$dataCatalogName.client.factory"),
        "org.apache.iceberg.aws.AssumeRoleAwsClientFactory"
      )
    }

    assertEquals(sparkSession.conf.get(f"spark.sql.catalog.$dataCatalogName.s3.sse.type"), "kms")
    assertEquals(sparkSession.conf.get(f"spark.sql.catalog.$dataCatalogName.warehouse"), resolvedOutputS3Uri)

    assertEquals(sparkSession.conf.get(f"spark.sql.catalog.$dataCatalogName"), "org.apache.iceberg.spark.SparkCatalog")
    assertEquals(
      sparkSession.conf.get(f"spark.sql.catalog.$dataCatalogName.catalog-impl"),
      "org.apache.iceberg.aws.glue.GlueCatalog"
    )
    assertEquals(
      sparkSession.conf.get(f"spark.sql.catalog.$dataCatalogName.io-impl"),
      "org.apache.iceberg.aws.s3.S3FileIO"
    )
    assertEquals(sparkSession.conf.get(f"spark.sql.catalog.$dataCatalogName.glue.skip-name-validation"), "true")
    assertEquals(sparkSession.conf.get(f"spark.sql.catalog.$dataCatalogName.glue.skip-archive"), "true")
  }

  @DataProvider
  def initializeSparkSessionForOfflineStoreDataProvider(): Array[Array[Any]] = {
    Array(
      Array("offline-store-kms-key-id", null, "us-west-2", "s3.us-west-2.amazonaws.com"),
      Array("offline-store-kms-key-id", "test-role-arn", "us-west-2", "s3.us-west-2.amazonaws.com"),
      Array(null, null, "cn-north-1", "s3.cn-north-1.amazonaws.com.cn")
    )
  }

  @DataProvider
  def initializeSparkSessionForIcebergTableDataProvider(): Array[Array[Any]] = {
    Array(
      Array("offline-store-kms-key-id", "s3-uri", "aws", null, "us-west-2"),
      Array(null, "s3-uri", "aws", "test-role-arn", "us-west-2"),
      Array("offline-store-kms-key-id", "s3-uri", "aws", "test-role-arn", "us-west-2")
    )
  }
}
