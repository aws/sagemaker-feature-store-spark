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

package com.amazonaws.services.sagemaker.featurestore.sparksdk

import com.amazonaws.services.sagemaker.featurestore.sparksdk.exceptions.ValidationError
import com.amazonaws.services.sagemaker.featurestore.sparksdk.helpers.FeatureGroupHelper._
import com.amazonaws.services.sagemaker.featurestore.sparksdk.helpers.{ClientFactory, DataFrameRepartitioner, FeatureGroupArnResolver, SparkSessionInitializer}
import com.amazonaws.services.sagemaker.featurestore.sparksdk.validators.InputDataSchemaValidator._
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, lit}
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType}

import collection.JavaConverters._
import org.apache.spark.sql.{DataFrame, Row}
import software.amazon.awssdk.services.sagemaker.model.{DescribeFeatureGroupRequest, DescribeFeatureGroupResponse, FeatureDefinition, FeatureType}
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.SageMakerFeatureStoreRuntimeClient
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.{FeatureValue, PutRecordRequest}

import java.util
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class FeatureStoreManager extends Serializable {

  val PARQUET_BLOCK_SIZE_IN_MB: Int = 128 * 1024 * 1024
  val SPARK_TYPE_TO_FEATURE_TYPE_MAP: Map[DataType, FeatureType] = Map(
    StringType  -> FeatureType.STRING,
    DoubleType  -> FeatureType.FRACTIONAL,
    FloatType   -> FeatureType.FRACTIONAL,
    ShortType   -> FeatureType.INTEGRAL,
    IntegerType -> FeatureType.INTEGRAL,
    LongType    -> FeatureType.INTEGRAL
  )

  /**
    * Batch ingest data into SageMaker FeatureStore.
    *
    * @param inputDataFrame input Spark DataFrame to be ingested.
    * @param featureGroupArn .arn of a feature group.
    * @param directOfflineStore choose if data should be only ingested to OfflineStore of a FeatureGroup.
    */
  def ingestData(inputDataFrame: DataFrame, featureGroupArn: String, directOfflineStore: Boolean = false): Unit = {

    var startTimeMillis = System.currentTimeMillis()

    SparkSessionInitializer.initialieSparkSession(inputDataFrame.sparkSession)

    var durationSeconds = (System.currentTimeMillis() - startTimeMillis) / 1000

    val featureGroupArnResolver = new FeatureGroupArnResolver(featureGroupArn)
    val featureGroupName        = featureGroupArnResolver.resolveFeatureGroupName()
    val region                  = featureGroupArnResolver.resolveRegion()

    val describeResponse = getFeatureGroup(featureGroupName)

    checkIfFeatureGroupArnIdentical(describeResponse, featureGroupArn)
    checkIfFeatureGroupIsCreated(describeResponse)

    val eventTimeFeatureName = describeResponse.eventTimeFeatureName()

    startTimeMillis = System.currentTimeMillis()

    val validatedInputDataFrame = validateSchema(inputDataFrame, describeResponse)

    durationSeconds = (System.currentTimeMillis() - startTimeMillis) / 1000
    println(s"time elapsed for data schema validation: $durationSeconds seconds")

    if (directOfflineStore || !isFeatureGroupOnlineStoreEnabled(describeResponse)) {
      batchIngestIntoOfflineStore(
        validatedInputDataFrame,
        describeResponse,
        eventTimeFeatureName,
        region
      )
    } else {
      streamIngestIntoOnlineStore(featureGroupName, validatedInputDataFrame)
    }
  }

  /**
    * Load feature definitions according to the schema of input data frame.
    *
    * @param inputDataFrame input Spark DataFrame to be loaded.
    * @return list of feature definitions.
    */
  def loadFeatureDefinitionsFromSchema(inputDataFrame: DataFrame): util.List[FeatureDefinition] = {
    val featureDefinitions: ListBuffer[FeatureDefinition] = ListBuffer()
    val fields                                            = inputDataFrame.schema.fields
    for (field <- fields) {
      featureDefinitions += FeatureDefinition
        .builder()
        .featureName(field.name)
        .featureType(SPARK_TYPE_TO_FEATURE_TYPE_MAP.getOrElse(field.dataType, default = FeatureType.STRING))
        .build()
    }
    featureDefinitions.asJava
  }

  private def streamIngestIntoOnlineStore(featureGroupName: String, inputDataFrame: DataFrame): Unit = {
    val columns = inputDataFrame.schema.names
    val repartitionedDataFrame = DataFrameRepartitioner.repartition(inputDataFrame)
    repartitionedDataFrame.foreachPartition((rows: Iterator[Row]) => {
      putOnlineRecordsForPartition(
        rows,
        featureGroupName,
        columns,
        ClientFactory.sageMakerFeatureStoreRuntimeClientBuilder.build()
      )
    })
  }

  private def putOnlineRecordsForPartition(
      rows: Iterator[Row],
      featureGroupName: String,
      columns: Array[String],
      runTimeClient: SageMakerFeatureStoreRuntimeClient
  ): Unit = {
    for (row <- rows) {
      val record = ListBuffer[FeatureValue]()
      for (columnName <- columns) {
        Try(row.getAs[String](columnName)) match {
          case Success(s) =>
            record += FeatureValue
              .builder()
              .featureName(columnName)
              .valueAsString(s)
              .build()
          case Failure(e) => throw new RuntimeException(e)
        }
      }
      Try {
        val putRecordRequest = PutRecordRequest
          .builder()
          .featureGroupName(featureGroupName)
          .record(record.asJava)
          .build()
        runTimeClient.putRecord(putRecordRequest)
      } match {
        case Success(s) =>
        case Failure(e) =>
          throw new RuntimeException(
            s"Failed to put record '$record', due to exception '$e'"
          )
      }
    }
  }

  private def batchIngestIntoOfflineStore(
      dataFrame: DataFrame,
      describeResponse: DescribeFeatureGroupResponse,
      eventTimeFeatureName: String,
      region: String
  ): Unit = {

    if (!isFeatureGroupOfflineStoreEnabled(describeResponse)) {
      throw ValidationError(
        s"OfflineStore of FeatureGroup: '${describeResponse.featureGroupName()}' is not enabled."
      )
    }

    val offlineStoreEncryptionKeyId =
      describeResponse.offlineStoreConfig().s3StorageConfig().kmsKeyId()

    SparkSessionInitializer.initializeSparkSessionForOfflineStore(
      dataFrame.sparkSession,
      offlineStoreEncryptionKeyId,
      region
    )

    val destinationFilePath = generateDestinationFilePath(describeResponse)
    var tempDataFrame = dataFrame.withColumn(
      "temp_event_time_col",
      col(eventTimeFeatureName).cast("Timestamp")
    )

    tempDataFrame = tempDataFrame
      .withColumn("year", date_format(col("temp_event_time_col"), "yyyy"))
      .withColumn("month", date_format(col("temp_event_time_col"), "MM"))
      .withColumn("day", date_format(col("temp_event_time_col"), "dd"))
      .withColumn("hour", date_format(col("temp_event_time_col"), "HH"))

    tempDataFrame = tempDataFrame.drop("temp_event_time_col")

    tempDataFrame = tempDataFrame
      .withColumn("api_invocation_time", current_timestamp())
      .withColumn("write_time", current_timestamp())

    tempDataFrame = tempDataFrame.withColumn("is_deleted", lit(false))

    tempDataFrame.write
      .partitionBy("year", "month", "day", "hour")
      .option("compression", "none")
      .option("parquet.block.size", PARQUET_BLOCK_SIZE_IN_MB)
      .mode("append")
      .parquet(destinationFilePath)
  }

  private def getFeatureGroup(featureGroupName: String): DescribeFeatureGroupResponse = {
    val describeRequest = DescribeFeatureGroupRequest
      .builder()
      .featureGroupName(featureGroupName)
      .build()
    ClientFactory.sageMakerClient.describeFeatureGroup(describeRequest)
  }
}
