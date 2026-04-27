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
import software.amazon.awssdk.services.lakeformation.model.{GetTemporaryGlueTableCredentialsRequest, Permission}
import scala.util.{Failure, Success, Try}

object LakeFormationHelper {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Credential duration is currently hardcoded. refreshIfNeeded vends fresh credentials before
  // each write, but once a Spark write begins the credentials are fixed for its duration. If a
  // single write exceeds 1 hour (e.g. very large DataFrames), executors will fail with S3 403
  // AccessDenied. Callers should batch large DataFrames into smaller writes to stay within the TTL.
  private val CREDENTIAL_DURATION_SECONDS = 3600
  private val REFRESH_BUFFER_SECONDS      = 300

  def vendCredentials(
      region: String,
      accountId: String,
      partition: String,
      database: String,
      table: String
  ): LakeFormationCredentials = {
    val tableArn = buildGlueTableArn(partition, region, accountId, database, table)
    logger.debug(s"Vending LF credentials for table ARN: $tableArn")
    Try {
      val response = ClientFactory.lakeFormationClient.getTemporaryGlueTableCredentials(
        GetTemporaryGlueTableCredentialsRequest
          .builder()
          .tableArn(tableArn)
          .permissions(Permission.SELECT, Permission.INSERT, Permission.DESCRIBE)
          .durationSeconds(CREDENTIAL_DURATION_SECONDS)
          .build()
      )
      LakeFormationCredentials(
        accessKeyId = response.accessKeyId(),
        secretAccessKey = response.secretAccessKey(),
        sessionToken = response.sessionToken(),
        expiration = response.expiration(),
        region = region,
        accountId = accountId,
        partition = partition,
        database = database,
        table = table
      )
    } match {
      case Success(creds) =>
        logger.debug(s"Vended LF credentials for $database.$table, expires at ${creds.expiration}")
        creds
      case Failure(ex) =>
        throw new RuntimeException(
          s"Failed to vend Lake Formation credentials for $database.$table. " +
            s"The caller explicitly requested Lake Formation credential vending but GetTemporaryGlueTableCredentials failed. " +
            s"Check that the IAM role has lakeformation:GetDataAccess and the Lake Formation table grants are configured correctly. " +
            s"See the connector's README troubleshooting section for details.",
          ex
        )
    }
  }

  def refreshIfNeeded(credentials: LakeFormationCredentials): LakeFormationCredentials = {
    if (credentials.isExpiringSoon(REFRESH_BUFFER_SECONDS)) {
      logger.debug(s"LF credentials expiring soon, refreshing for ${credentials.database}.${credentials.table}")
      vendCredentials(
        credentials.region,
        credentials.accountId,
        credentials.partition,
        credentials.database,
        credentials.table
      )
    } else {
      credentials
    }
  }

  private[helpers] def buildGlueTableArn(
      partition: String,
      region: String,
      accountId: String,
      database: String,
      table: String
  ): String = {
    s"arn:$partition:glue:$region:$accountId:table/$database/$table"
  }

  /** Seed the LF-registered prefix with a tiny marker object using the Hadoop FileSystem that was just configured with
   *  LF-vended creds. This ensures the subsequent S3A mkdirs probe during committer setupJob finds a non-empty LIST and
   *  skips the HEAD on the prefix-as-object that LF denies with 403. Best-effort: failures are logged and ignored.
   */
  def seedLfPrefix(sparkSession: SparkSession, resolvedOutputS3Uri: String): Unit = {
    import org.apache.hadoop.fs.{FileSystem, Path}
    import java.net.URI

    val s3aUri = resolvedOutputS3Uri.replaceFirst("^s3:", "s3a:").stripSuffix("/")
    try {
      val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
      val fs         = FileSystem.get(new URI(s3aUri), hadoopConf)
      val markerPath = new Path(s"$s3aUri/_feature_store_spark_init")
      val out        = fs.create(markerPath, /* overwrite */ true)
      try out.close()
      catch { case _: Throwable => () }
      logger.info(s"Seeded LF-registered prefix with marker $markerPath")
    } catch {
      case t: Throwable =>
        logger.warn(s"Failed to seed LF-registered prefix $s3aUri: ${t.getMessage}")
    }
  }

}
