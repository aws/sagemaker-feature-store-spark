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

import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.lakeformation.model.{GetTemporaryGlueTableCredentialsRequest, Permission}

import scala.util.{Failure, Success, Try}

object LakeFormationHelper {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Credential duration is currently hardcoded. For long-running Spark jobs exceeding 1 hour,
  // credentials will be refreshed automatically via refreshIfNeeded before they expire.
  private val CREDENTIAL_DURATION_SECONDS = 3600
  private val REFRESH_BUFFER_SECONDS      = 300

  def vendCredentials(
      region: String,
      accountId: String,
      partition: String,
      database: String,
      table: String
  ): Option[LakeFormationCredentials] = {
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
        Some(creds)
      case Failure(ex) =>
        logger.warn(
          s"Failed to vend LF credentials for $database.$table, falling back to default credentials: ${ex.getMessage}"
        )
        None
    }
  }

  def refreshIfNeeded(credentials: LakeFormationCredentials): Option[LakeFormationCredentials] = {
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
      Some(credentials)
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

  private def buildPartitionFromRegion(region: String): String = {
    if (region.startsWith("cn-")) "aws-cn"
    else if (region.startsWith("us-gov-")) "aws-us-gov"
    else "aws"
  }
}
