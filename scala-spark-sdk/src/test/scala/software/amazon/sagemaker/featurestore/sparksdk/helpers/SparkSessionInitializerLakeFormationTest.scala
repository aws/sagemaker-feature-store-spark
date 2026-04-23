package software.amazon.sagemaker.featurestore.sparksdk.helpers

import org.apache.spark.sql.SparkSession
import org.scalatestplus.testng.TestNGSuite
import org.testng.Assert.{assertEquals, assertNotEquals, assertTrue, fail}
import org.testng.annotations.Test

import java.time.Instant

/** Branch coverage for [[SparkSessionInitializer]]'s LF credential wiring and magic-committer runtime detection.
 *
 *  Lives in the base test source set so all Spark builds exercise these paths. The three branches of
 *  `configureMagicCommitter` are exercised directly via the injectable class-presence predicate; the LF branches of
 *  `initializeSparkSessionFor{OfflineStore,IcebergTable}` are exercised by calling them with `Some(creds)` and
 *  tolerating the downstream magic-committer outcome (success on 3.5+, IllegalStateException on 3.1-3.4 where neither
 *  committer impl is on the classpath).
 */
class SparkSessionInitializerLakeFormationTest extends TestNGSuite {

  private final val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("TestProgram")
    .master("local")
    .getOrCreate()

  private final val EMR_COMMIT_PROTOCOL =
    "org.apache.spark.sql.execution.datasources.SQLEmrOptimizedCommitProtocol"
  private final val PATH_OUTPUT_COMMIT_PROTOCOL =
    "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol"
  private final val BINDING_PARQUET_COMMITTER =
    "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter"

  private def freshCreds(suffix: String): LakeFormationCredentials = LakeFormationCredentials(
    accessKeyId = s"ak-$suffix",
    secretAccessKey = s"sk-$suffix",
    sessionToken = s"st-$suffix",
    expiration = Instant.now().plusSeconds(3600),
    region = "us-west-2",
    accountId = "123456789012",
    partition = "aws",
    database = "db",
    table = "tbl"
  )

  private def runIgnoringCommitProtocolMissing(body: => Unit): Unit =
    try body
    catch {
      // On Spark 3.1-3.4 builds, neither SQLEmrOptimizedCommitProtocol (EMR) nor PathOutputCommitProtocol
      // (spark-hadoop-cloud) is on the classpath, so configureMagicCommitter fails fast. We still want
      // the credential-setting lines above the committer call to be exercised, so treat that specific
      // failure as expected; everything else is a real test failure.
      case e: IllegalStateException if e.getMessage.contains("magic committer") =>
      case e: IllegalStateException
          if e.getMessage
            .contains("PathOutputCommitProtocol") || e.getMessage.contains("SQLEmrOptimizedCommitProtocol") =>
    }

  @Test
  def configureMagicCommitterOnEmrLeavesCommitProtocolClassUnsetTest(): Unit = {
    sparkSession.conf.unset("spark.sql.sources.commitProtocolClass")
    sparkSession.conf.unset("spark.sql.parquet.output.committer.class")

    SparkSessionInitializer.configureMagicCommitter(sparkSession, _ == EMR_COMMIT_PROTOCOL)

    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    assertEquals(hadoopConf.get("fs.s3a.committer.name"), "magic")
    assertEquals(hadoopConf.get("fs.s3a.committer.magic.enabled"), "true")
    // On EMR the SDK must NOT override Spark's commit protocol with the open-source class.
    assertNotEquals(
      sparkSession.conf.get("spark.sql.sources.commitProtocolClass", ""),
      PATH_OUTPUT_COMMIT_PROTOCOL
    )
    assertNotEquals(
      sparkSession.conf.get("spark.sql.parquet.output.committer.class", ""),
      BINDING_PARQUET_COMMITTER
    )
  }

  @Test
  def configureMagicCommitterOnNonEmrSetsOpenSourceCommitProtocolTest(): Unit = {
    sparkSession.conf.unset("spark.sql.sources.commitProtocolClass")
    sparkSession.conf.unset("spark.sql.parquet.output.committer.class")

    SparkSessionInitializer.configureMagicCommitter(sparkSession, _ == PATH_OUTPUT_COMMIT_PROTOCOL)

    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    assertEquals(hadoopConf.get("fs.s3a.committer.name"), "magic")
    assertEquals(sparkSession.conf.get("spark.sql.sources.commitProtocolClass"), PATH_OUTPUT_COMMIT_PROTOCOL)
    assertEquals(sparkSession.conf.get("spark.sql.parquet.output.committer.class"), BINDING_PARQUET_COMMITTER)
  }

  @Test
  def configureMagicCommitterWhenNeitherAvailableThrowsTest(): Unit = {
    try {
      SparkSessionInitializer.configureMagicCommitter(sparkSession, _ => false)
      fail("Expected IllegalStateException when no magic commit protocol is on classpath")
    } catch {
      case e: IllegalStateException =>
        assertTrue(e.getMessage.contains("spark-hadoop-cloud"), e.getMessage)
        assertTrue(e.getMessage.contains("EMR"), e.getMessage)
    }
  }

  @Test
  def initializeSparkSessionForOfflineStoreWithLfCredentialsSetsS3aConfigsTest(): Unit = {
    val creds = freshCreds("off")
    runIgnoringCommitProtocolMissing {
      SparkSessionInitializer.initializeSparkSessionForOfflineStore(
        sparkSession,
        "kms-key",
        null,
        "us-west-2",
        Some(creds)
      )
    }
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    assertEquals(
      hadoopConf.get("fs.s3a.aws.credentials.provider"),
      "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
    )
    assertEquals(hadoopConf.get("fs.s3a.access.key"), "ak-off")
    assertEquals(hadoopConf.get("fs.s3a.secret.key"), "sk-off")
    assertEquals(hadoopConf.get("fs.s3a.session.token"), "st-off")
  }

  @Test
  def initializeSparkSessionForIcebergTableWithLfCredentialsSetsS3aConfigsTest(): Unit = {
    val creds = freshCreds("ice")
    runIgnoringCommitProtocolMissing {
      SparkSessionInitializer.initializeSparkSessionForIcebergTable(
        sparkSession,
        "kms-key",
        "s3://bucket/path",
        "lf_catalog",
        null,
        "us-west-2",
        Some(creds)
      )
    }
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    assertEquals(
      hadoopConf.get("fs.s3a.aws.credentials.provider"),
      "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
    )
    assertEquals(hadoopConf.get("fs.s3a.access.key"), "ak-ice")
    assertEquals(hadoopConf.get("fs.s3a.secret.key"), "sk-ice")
    assertEquals(hadoopConf.get("fs.s3a.session.token"), "st-ice")
  }
}
