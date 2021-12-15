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

  @Test
  def initializeSparkSessionTest(): Unit = {
    SparkSessionInitializer.initializeSparkSession(sparkSession)

    assertEquals(
      sparkSession.sparkContext.hadoopConfiguration
        .get("fs.s3a.aws.credentials.provider"),
      "com.amazonaws.auth.ContainerCredentialsProvider," +
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
  }

  @Test(dataProvider = "initializeSparkSessionForOfflineStoreDataProvider")
  def initializeSparkSessionForOfflineStoreTest(
      offlineStoreKmsKeyId: String,
      region: String,
      expectedS3Endpoint: String
  ): Unit = {
    SparkSessionInitializer.initializeSparkSessionForOfflineStore(
      sparkSession,
      offlineStoreKmsKeyId,
      region
    )

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
    }
  }

  @DataProvider
  def initializeSparkSessionForOfflineStoreDataProvider(): Array[Array[Any]] = {
    Array(
      Array("offline-store-kms-key-id", "us-west-2", null),
      Array(null, "cn-north-1", "s3.cn-north-1.amazonaws.com.cn")
    )
  }
}
