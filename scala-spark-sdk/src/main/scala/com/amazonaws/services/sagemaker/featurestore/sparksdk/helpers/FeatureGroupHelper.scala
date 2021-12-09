/*
 *  Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazonaws.services.sagemaker.featurestore.sparksdk.helpers

import com.amazonaws.services.sagemaker.featurestore.sparksdk.exceptions.ValidationError
import software.amazon.awssdk.services.sagemaker.model.{DescribeFeatureGroupResponse, FeatureGroupStatus}

object FeatureGroupHelper {

  /** Check if FeatureGroup is in "Created" status.
    *
    * @param describeResponse response of DescribeFeatureGroup.
    */
  def checkIfFeatureGroupIsCreated(describeResponse: DescribeFeatureGroupResponse): Unit = {
    if (!FeatureGroupStatus.CREATED.equals(describeResponse.featureGroupStatus())) {
      throw ValidationError(
        s"Feature group '${describeResponse.featureGroupName()}' is in " +
          s"'${describeResponse.featureGroupStatus()}' status, " +
          s"however status must be in 'Created' instead"
      )
    }
  }

  /** Check if FeatureGroup is in "Created" status.
    *
   * @param describeResponse response of DescribeFeatureGroup.
    */
  def checkIfFeatureGroupArnIdentical(
      describeResponse: DescribeFeatureGroupResponse,
      providedFeatureGroupArn: String
  ): Unit = {
    if (describeResponse.featureGroupArn() != providedFeatureGroupArn) {
      throw ValidationError(
        s"Provided feature group arn does not match the arn detected." +
          s" For now cross account or region ingestion is not supported."
      )
    }
  }

  /** Check if directOfflineStore is set correctly.
   *
   * @param describeResponse response of DescribeFeatureGroup.
   */
  def checkDirectOfflineStore(
      describeResponse: DescribeFeatureGroupResponse,
      directOfflineStore: Boolean
  ): Unit = {
    if (!isFeatureGroupOfflineStoreEnabled(describeResponse) && directOfflineStore) {
      throw ValidationError(
        s"OfflineStore of FeatureGroup: '${describeResponse.featureGroupName()}' is not enabled, however directOfflineStore is set to 'true'."
      )
    }
  }

  /** Check if FeatureGruop has OnlineStore enabled.
    *
   * @param describeResponse response of DescribeFeatureGroup.
    * @return true if OnlineStore of FeatureGroup is enabled.
    */
  def isFeatureGroupOnlineStoreEnabled(describeResponse: DescribeFeatureGroupResponse): Boolean = {
    val onlineStoreConfig = describeResponse.onlineStoreConfig()

    onlineStoreConfig != null && onlineStoreConfig.enableOnlineStore()
  }

  /** Check if FeatureGruop has OfflineStore enabled.
    *
    * @param describeResponse response of DescribeFeatureGroup.
    * @return true if OfflineStore of FeatureGroup is enabled.
    */
  def isFeatureGroupOfflineStoreEnabled(describeResponse: DescribeFeatureGroupResponse): Boolean = {
    val offlineStoreConfig = describeResponse.offlineStoreConfig()

    offlineStoreConfig != null
  }

  /** Generate the destination file path of output data.
    *
    * @param describeResponse response of DescribeFeatureGroup.
    * @return The generated S3 ouput path of data files.
    */
  def generateDestinationFilePath(describeResponse: DescribeFeatureGroupResponse): String = {
    val resolvedOutputS3Uri = describeResponse
      .offlineStoreConfig()
      .s3StorageConfig()
      .resolvedOutputS3Uri()

    resolvedOutputS3Uri.replaceFirst("s3", "s3a")
  }
}
