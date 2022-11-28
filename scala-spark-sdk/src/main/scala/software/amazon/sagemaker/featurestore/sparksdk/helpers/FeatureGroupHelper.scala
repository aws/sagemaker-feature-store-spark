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

import software.amazon.awssdk.services.sagemaker.model.{DescribeFeatureGroupResponse, FeatureGroupStatus, TableFormat}
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.TargetStore
import software.amazon.sagemaker.featurestore.sparksdk.exceptions.ValidationError

import scala.collection.mutable.ListBuffer

object FeatureGroupHelper {

  /** Check if FeatureGroup is in "Created" status.
   *
   *  @param describeResponse
   *    response of DescribeFeatureGroup.
   */
  def checkIfFeatureGroupIsCreated(describeResponse: DescribeFeatureGroupResponse): Unit = {
    if (!FeatureGroupStatus.CREATED.equals(describeResponse.featureGroupStatus())) {
      throw ValidationError(
        s"Feature group '${describeResponse.featureGroupName()}' is in " +
          s"'${describeResponse.featureGroupStatus()}' status, " +
          s"however status must be in 'Created' instead."
      )
    }
  }

  /** Check if directOfflineStore is set correctly.
   *
   *  @param describeResponse
   *    response of DescribeFeatureGroup.
   */
  def checkAndParseTargetStore(
      describeResponse: DescribeFeatureGroupResponse,
      targetStores: List[String]
  ): List[TargetStore] = {

    // Skip the check if target store is null, then use default approach for ingestion based on
    // the configuration of feature group

    if (targetStores == null) {
      return null
    }

    val parsedStores = targetStores
      .map(store =>
        if (TargetStore.fromValue(store) == null) TargetStore.UNKNOWN_TO_SDK_VERSION
        else TargetStore.fromValue(store)
      )
      .toSet

    if (parsedStores.contains(TargetStore.UNKNOWN_TO_SDK_VERSION)) {
      throw ValidationError(
        s"Found unknown target store, the valid values are [${TargetStore.knownValues().toArray.mkString(", ")}]."
      )
    }

    if (parsedStores.size != targetStores.size) {
      throw ValidationError(s"Found duplicate target store, please remove duplicate value and try again.")
    }

    if (parsedStores.isEmpty) {
      throw ValidationError(
        "Target stores cannot be empty, please provide at least one target store or leave it unspecified."
      )
    }

    val invalidTargetStoresList = new ListBuffer[TargetStore]()

    // Online store is specified as target store however online store is not enabled
    if (parsedStores.contains(TargetStore.ONLINE_STORE) && !isFeatureGroupOnlineStoreEnabled(describeResponse)) {
      invalidTargetStoresList += TargetStore.ONLINE_STORE
    }

    // Offline store is specified as target store however offline store is not enabled
    if (parsedStores.contains(TargetStore.OFFLINE_STORE) && !isFeatureGroupOfflineStoreEnabled(describeResponse)) {
      invalidTargetStoresList += TargetStore.OFFLINE_STORE
    }

    if (invalidTargetStoresList.nonEmpty) {
      throw ValidationError(
        s"[${invalidTargetStoresList.mkString(",")}] of FeatureGroup: '${describeResponse.featureGroupName()}' are not enabled, however they are specified in target store."
      )
    }

    parsedStores.toList
  }

  /** Check if FeatureGruop has OnlineStore enabled.
   *
   *  @param describeResponse
   *    response of DescribeFeatureGroup.
   *  @return
   *    true if OnlineStore of FeatureGroup is enabled.
   */
  def isFeatureGroupOnlineStoreEnabled(describeResponse: DescribeFeatureGroupResponse): Boolean = {
    val onlineStoreConfig = Option(describeResponse.onlineStoreConfig())

    onlineStoreConfig.nonEmpty && onlineStoreConfig.get.enableOnlineStore()
  }

  /** Check if FeatureGruop has OfflineStore enabled.
   *
   *  @param describeResponse
   *    response of DescribeFeatureGroup.
   *  @return
   *    true if OfflineStore of FeatureGroup is enabled.
   */
  def isFeatureGroupOfflineStoreEnabled(describeResponse: DescribeFeatureGroupResponse): Boolean = {
    val offlineStoreConfig = Option(describeResponse.offlineStoreConfig())

    offlineStoreConfig.nonEmpty
  }

  /** Check if FeatureGruop has iceberg table enabled.
   *
   *  @param describeResponse
   *    response of DescribeFeatureGroup.
   *  @return
   *    true if iceberg table of FeatureGroup is enabled.
   */
  def isIcebergTableEnabled(describeResponse: DescribeFeatureGroupResponse): Boolean = {
    val offlineStoreConfig = Option(describeResponse.offlineStoreConfig())

    offlineStoreConfig.nonEmpty && TableFormat.ICEBERG.equals(offlineStoreConfig.get.tableFormat())
  }

  /** Check if FeatureGruop has glue table enabled.
   *
   *  @param describeResponse
   *    response of DescribeFeatureGroup.
   *  @return
   *    true if glue table of FeatureGroup is enabled.
   */
  def isGlueTableEnabled(describeResponse: DescribeFeatureGroupResponse): Boolean = {
    val offlineStoreConfig = Option(describeResponse.offlineStoreConfig())

    offlineStoreConfig.nonEmpty && TableFormat.GLUE.equals(offlineStoreConfig.get.tableFormat())
  }

  /** Generate the destination file path of output data.
   *
   *  @param describeResponse
   *    response of DescribeFeatureGroup.
   *  @return
   *    the generated S3 ouput path of data files.
   */
  def generateDestinationFilePath(describeResponse: DescribeFeatureGroupResponse): String = {
    val resolvedOutputS3Uri = describeResponse
      .offlineStoreConfig()
      .s3StorageConfig()
      .resolvedOutputS3Uri()

    resolvedOutputS3Uri.replaceFirst("s3", "s3a")
  }

  /** Get target stores from configuration of feature group
   *
   *  @param describeResponse
   *    response of DescribeFeatureGroup.
   *  @return
   *    target stores
   */
  def retrieveTargetStoresFromFeatureGroup(describeResponse: DescribeFeatureGroupResponse): List[TargetStore] = {
    val targetStores = new ListBuffer[TargetStore]();

    if (isFeatureGroupOnlineStoreEnabled(describeResponse)) {
      targetStores += TargetStore.ONLINE_STORE
    }

    if (isFeatureGroupOfflineStoreEnabled(describeResponse)) {
      targetStores += TargetStore.OFFLINE_STORE
    }

    targetStores.toList
  }
}
