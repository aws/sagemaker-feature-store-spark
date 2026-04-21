/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *      http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 */

package software.amazon.sagemaker.featurestore.sparksdk.helpers

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkSessionInitializer {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Fail-fast configuration of the S3A magic committer for Spark parquet writes.
  // Required whenever Lake Formation vends credentials scoped to a single S3 prefix:
  // the default FileOutputCommitter performs rename() and _temporary/ cleanup that
  // touches paths outside the scoped prefix, causing 403 errors. The magic committer
  // uses S3 multipart uploads and stays strictly within the destination prefix.
  //
  // Requires spark-hadoop-cloud on the classpath (bundled on EMR/Glue; for standalone
  // PySpark add `--packages org.apache.spark:spark-hadoop-cloud_2.12:<spark-version>`).
  private val PATH_OUTPUT_COMMIT_PROTOCOL = "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol"
  private val BINDING_PARQUET_COMMITTER   = "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter"

  private def configureMagicCommitter(sparkSession: SparkSession): Unit = {
    // Use the thread context classloader so we see jars loaded via spark.jars.packages,
    // not just the classloader that loaded this SDK.
    val classLoader =
      Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
    try {
      Class.forName(PATH_OUTPUT_COMMIT_PROTOCOL, false, classLoader)
    } catch {
      case _: ClassNotFoundException =>
        val msg =
          s"$PATH_OUTPUT_COMMIT_PROTOCOL not found on classpath. Lake Formation credential " +
            "vending requires the S3A magic committer, which is provided by spark-hadoop-cloud. " +
            "Add it via spark-submit: --packages org.apache.spark:spark-hadoop-cloud_2.12:<spark-version>"
        logger.error(msg)
        throw new IllegalStateException(msg)
    }
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3a.committer.name", "magic")
    hadoopConf.set("fs.s3a.committer.magic.enabled", "true")
    hadoopConf.set(
      "mapreduce.outputcommitter.factory.scheme.s3a",
      "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"
    )
    sparkSession.conf.set("spark.sql.sources.commitProtocolClass", PATH_OUTPUT_COMMIT_PROTOCOL)
    sparkSession.conf.set("spark.sql.parquet.output.committer.class", BINDING_PARQUET_COMMITTER)
  }

  /** Initialize the spark session for offline store
   *
   *  @param sparkSession
   *    spark session to be initialized
   *  @param offlineStoreEncryptionKmsKeyId
   *    Kms key specified in offline store configuration of feature group
   */
  def initializeSparkSessionForOfflineStore(
      sparkSession: SparkSession,
      offlineStoreEncryptionKmsKeyId: String,
      assumeRoleArn: String,
      region: String,
      lfCredentials: Option[LakeFormationCredentials] = None
  ): Unit = {

    // Initialize hadoop configurations based on credential source
    // 1. If LF credentials are provided, use temporary credentials from Lake Formation
    // 2. If role arn is not provided, use default credential provider
    // For more info: https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html
    // 3. If role arn is provided, use assumed role arn provider instead
    // For more info: https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/assumed_roles.html

    lfCredentials match {
      case Some(creds) =>
        sparkSession.sparkContext.hadoopConfiguration
          .set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", creds.accessKeyId)
        sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", creds.secretAccessKey)
        sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", creds.sessionToken)
      case None =>
        val local_credentials_provider = List(
          "com.amazonaws.auth.ContainerCredentialsProvider",
          "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        ).mkString(",")
        if (assumeRoleArn == null) {
          sparkSession.sparkContext.hadoopConfiguration
            .set("fs.s3a.aws.credentials.provider", local_credentials_provider)
        } else {
          val credentials_provider = "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"
          sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", credentials_provider)
          sparkSession.sparkContext.hadoopConfiguration
            .set("fs.s3a.assumed.role.credentials.provider", local_credentials_provider)
          sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.assumed.role.arn", assumeRoleArn)
        }
    }

    sparkSession.sparkContext.hadoopConfiguration
      .set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    sparkSession.sparkContext.hadoopConfiguration
      .set("parquet.summary.metadata.level", "NONE")

    // feature store uses SSE-KMS to encrypt data file written to S3, if no kms key id is
    // specified, by default an AWS managed CMK of s3 will be used.
    // For more info: https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html
    sparkSession.sparkContext.hadoopConfiguration
      .set("fs.s3a.server-side-encryption-algorithm", "SSE-KMS")

    if (offlineStoreEncryptionKmsKeyId != null) {
      sparkSession.sparkContext.hadoopConfiguration.set(
        "fs.s3a.server-side-encryption.key",
        offlineStoreEncryptionKmsKeyId
      )
    }

    if (region.startsWith("cn-")) {
      sparkSession.sparkContext.hadoopConfiguration
        .set("fs.s3a.endpoint", s"s3.$region.amazonaws.com.cn")
    } else {
      sparkSession.sparkContext.hadoopConfiguration
        .set("fs.s3a.endpoint", s"s3.$region.amazonaws.com")
    }

    // Parquet writes with LF-vended credentials require the S3A magic committer because
    // the default FileOutputCommitter touches paths outside the LF-scoped prefix.
    if (lfCredentials.isDefined) {
      configureMagicCommitter(sparkSession)
    }
  }

  /** Initialize the spark session for offline store for iceberg table Fore more info:
   *  https://iceberg.apache.org/docs/latest/aws/
   *  @param sparkSession
   *    Spark session to be initialized
   *  @param offlineStoreEncryptionKmsKeyId
   *    Kms key specified in offline store configuration of feature group
   *  @param resolvedOutputS3Uri
   *    Offline store s3 uri
   *  @param assumeRoleArn
   *    Role arn to be assumed for offline ingestion to iceberg table
   *  @param region
   *    Region
   */
  def initializeSparkSessionForIcebergTable(
      sparkSession: SparkSession,
      offlineStoreEncryptionKmsKeyId: String,
      resolvedOutputS3Uri: String,
      dataCatalogName: String,
      assumeRoleArn: String,
      region: String,
      lfCredentials: Option[LakeFormationCredentials] = None
  ): Unit = {

    if (offlineStoreEncryptionKmsKeyId != null) {
      sparkSession.conf.set(f"spark.sql.catalog.$dataCatalogName.s3.sse.key", offlineStoreEncryptionKmsKeyId)
    }

    lfCredentials match {
      case Some(creds) =>
        sparkSession.sparkContext.hadoopConfiguration
          .set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", creds.accessKeyId)
        sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", creds.secretAccessKey)
        sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", creds.sessionToken)
      case None =>
        if (assumeRoleArn != null) {
          sparkSession.conf.set(f"spark.sql.catalog.$dataCatalogName.client.assume-role.arn", assumeRoleArn)
          sparkSession.conf.set(f"spark.sql.catalog.$dataCatalogName.client.assume-role.region", region)
          sparkSession.conf.set(
            f"spark.sql.catalog.$dataCatalogName.client.factory",
            "org.apache.iceberg.aws.AssumeRoleAwsClientFactory"
          )
        }
    }

    sparkSession.conf.set(f"spark.sql.catalog.$dataCatalogName.s3.sse.type", "kms")
    sparkSession.conf.set(f"spark.sql.catalog.$dataCatalogName.warehouse", resolvedOutputS3Uri)
    sparkSession.conf.set(f"spark.sql.catalog.$dataCatalogName", "org.apache.iceberg.spark.SparkCatalog")
    sparkSession.conf.set(f"spark.sql.catalog.$dataCatalogName.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    sparkSession.conf.set(f"spark.sql.catalog.$dataCatalogName.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    sparkSession.conf.set(f"spark.sql.catalog.$dataCatalogName.glue.skip-name-validation", "true")
    sparkSession.conf.set(f"spark.sql.catalog.$dataCatalogName.glue.skip-archive", "true")
  }
}
