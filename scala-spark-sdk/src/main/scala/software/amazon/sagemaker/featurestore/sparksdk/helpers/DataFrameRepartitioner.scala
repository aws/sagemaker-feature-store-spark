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

import org.apache.spark.sql.DataFrame

object DataFrameRepartitioner {

  val DEFAULT_SHUFFLE_PARTITIONS: String = "200"

  def getParallelism(inputDataFrame: DataFrame): Int = {
    val sparkContext          = inputDataFrame.sparkSession.sparkContext
    val configuredParallelism = sparkContext.getConf.get("spark.default.parallelism", null)
    val systemParallelism     = sparkContext.defaultParallelism

    // Worship customer's configuration, else return the parallelism calculated from number of total cores
    if (configuredParallelism != null) {
      configuredParallelism.toInt
    } else {
      systemParallelism
    }
  }

  def repartition(inputDataFrame: DataFrame): DataFrame = {
    val sparkContext    = inputDataFrame.sparkSession.sparkContext
    val parallelism     = getParallelism(inputDataFrame)
    val maxPartitions   = sparkContext.getConf.get("spark.sql.shuffle.partitions", DEFAULT_SHUFFLE_PARTITIONS).toInt
    val partitionsCount = inputDataFrame.rdd.getNumPartitions

    // Repartitioning is a very costly operation, only do this when:
    // 1. Too few partitions, even less than parallelism
    // 2. Too many partitions, even greater than number of shuffle partitions
    if (partitionsCount < parallelism || partitionsCount > maxPartitions) {
      inputDataFrame.repartition(parallelism)
    } else {
      inputDataFrame
    }
  }

}
