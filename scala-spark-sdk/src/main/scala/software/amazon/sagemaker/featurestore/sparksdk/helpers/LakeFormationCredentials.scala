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

import java.time.Instant

case class LakeFormationCredentials(
    accessKeyId: String,
    secretAccessKey: String,
    sessionToken: String,
    expiration: Instant,
    region: String,
    accountId: String,
    partition: String,
    database: String,
    table: String
) {
  def isExpiringSoon(bufferSeconds: Int): Boolean = {
    Instant.now().plusSeconds(bufferSeconds).isAfter(expiration)
  }

  override def toString: String =
    s"LakeFormationCredentials(accessKeyId=${accessKeyId.take(4)}***, expiration=$expiration, database=$database, table=$table)"
}
