package software.amazon.sagemaker.featurestore.sparksdk.helpers

import org.apache.spark.sql.SparkSession
import org.scalatestplus.testng.TestNGSuite
import org.testng.Assert.assertEquals
import org.testng.annotations.Test

import java.time.Instant

/** Lake Formation specific tests for SparkSessionInitializer. Only compiled for Spark 3.5+ because LF support is gated
 *  to that version.
 */
class SparkSessionInitializerLakeFormationTest extends TestNGSuite {

  private final val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("TestProgram")
    .master("local")
    .getOrCreate()

  @Test
  def initializeSparkSessionForOfflineStoreWithLfCredentialsTest(): Unit = {
    val creds = LakeFormationCredentials(
      accessKeyId = "lf-ak",
      secretAccessKey = "lf-sk",
      sessionToken = "lf-st",
      expiration = Instant.now().plusSeconds(3600),
      region = "us-west-2",
      accountId = "123456789012",
      partition = "aws",
      database = "db",
      table = "tbl"
    )

    SparkSessionInitializer.initializeSparkSessionForOfflineStore(
      sparkSession,
      "kms-key",
      null,
      "us-west-2",
      Some(creds)
    )

    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    assertEquals(
      hadoopConf.get("fs.s3a.aws.credentials.provider"),
      "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
    )
    assertEquals(hadoopConf.get("fs.s3a.access.key"), "lf-ak")
    assertEquals(hadoopConf.get("fs.s3a.secret.key"), "lf-sk")
    assertEquals(hadoopConf.get("fs.s3a.session.token"), "lf-st")
  }

  @Test
  def initializeSparkSessionForIcebergTableWithLfCredentialsTest(): Unit = {
    val creds = LakeFormationCredentials(
      accessKeyId = "lf-ak2",
      secretAccessKey = "lf-sk2",
      sessionToken = "lf-st2",
      expiration = Instant.now().plusSeconds(3600),
      region = "us-west-2",
      accountId = "123456789012",
      partition = "aws",
      database = "db",
      table = "tbl"
    )

    SparkSessionInitializer.initializeSparkSessionForIcebergTable(
      sparkSession,
      "kms-key",
      "s3://bucket/path",
      "lf_catalog",
      null,
      "us-west-2",
      Some(creds)
    )

    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    assertEquals(
      hadoopConf.get("fs.s3a.aws.credentials.provider"),
      "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
    )
    assertEquals(hadoopConf.get("fs.s3a.access.key"), "lf-ak2")
    assertEquals(hadoopConf.get("fs.s3a.secret.key"), "lf-sk2")
    assertEquals(hadoopConf.get("fs.s3a.session.token"), "lf-st2")
  }
}
