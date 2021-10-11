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

import org.apache.spark.sql.SparkSession

object SparkSessionInitializer {

  /**
    * Initialize the spark session
    *
   * @param sparkSession spark session to be initialized
    */
  def initialieSparkSession(sparkSession: SparkSession): Unit = {
    val credentials_provider = List(
      "com.amazonaws.auth.ContainerCredentialsProvider",
      "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    ).mkString(",")

    sparkSession.sparkContext.hadoopConfiguration
      .set("fs.s3a.aws.credentials.provider", credentials_provider)
  }

  /**
    * Initialize the spark session for offline store
    *
   * @param sparkSession spark session to be initialized
    * @param offlineStoreEncryptionKmsKeyId KmsKey specified in offline store configuraiton of feature group
    */
  def initializeSparkSessionForOfflineStore(
      sparkSession: SparkSession,
      offlineStoreEncryptionKmsKeyId: String,
      region: String
  ): Unit = {
    sparkSession.sparkContext.hadoopConfiguration
      .set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    sparkSession.sparkContext.hadoopConfiguration
      .set("parquet.summary.metadata.level", "NONE")

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
    }
  }
}
