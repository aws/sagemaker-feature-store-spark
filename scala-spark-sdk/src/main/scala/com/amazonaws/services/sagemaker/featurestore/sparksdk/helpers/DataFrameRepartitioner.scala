package com.amazonaws.services.sagemaker.featurestore.sparksdk.helpers

import org.apache.spark.sql.DataFrame

object DataFrameRepartitioner {

  val DEFAULT_SHUFFLE_PARTITIONS: String = "200"

  def getParallelism(inputDataFrame: DataFrame): Int = {
    val sparkContext = inputDataFrame.sparkSession.sparkContext
    val configuredParallelism = sparkContext.getConf.get("spark.default.parallelism", null)
    val systemParallelism = sparkContext.defaultParallelism

    // Worship customer's configuration, else return the parallelism calculated from number of total cores
    if (configuredParallelism != null) {
      configuredParallelism.toInt
    } else {
      systemParallelism
    }
  }

  def repartition(inputDataFrame: DataFrame): DataFrame = {
    val sparkContext = inputDataFrame.sparkSession.sparkContext
    val parallelism = getParallelism(inputDataFrame)
    val maxPartitions = sparkContext.getConf.get("spark.sql.shuffle.partitions", DEFAULT_SHUFFLE_PARTITIONS).toInt
    val partitionsCount = inputDataFrame.rdd.getNumPartitions

    // Repartitioning is a very costly operation, only do this when:
    // 1. Too few partitions, even less than parallelism
    // 2. Too many partitions, even greater than number of shuffle partitions
    if (partitionsCount < parallelism || partitionsCount > maxPartitions) {
      inputDataFrame.repartition(parallelism)
    }
  }

}
