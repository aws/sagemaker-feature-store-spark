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

  @DataProvider
  def initializeSparkSessionForOfflineStoreDataProvider(): Array[Array[Any]] = {
    Array(
      Array("offline-store-kms-key-id", null, "us-west-2", "s3.us-west-2.amazonaws.com"),
      Array("offline-store-kms-key-id", "test-role-arn", "us-west-2", "s3.us-west-2.amazonaws.com"),
      Array(null, null, "cn-north-1", "s3.cn-north-1.amazonaws.com.cn")
    )
  }
}
