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

package software.amazon.sagemaker.featurestore.sparksdk

import software.amazon.sagemaker.featurestore.sparksdk.helpers.FeatureGroupHelper._
import software.amazon.sagemaker.featurestore.sparksdk.validators.InputDataSchemaValidator._
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, lit}
import org.apache.spark.sql.types.{
  ByteType,
  DataType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  ShortType,
  StringType
}

import collection.JavaConverters._
import org.apache.spark.sql.{DataFrame, Row}
import software.amazon.awssdk.services.sagemaker.model.{
  DescribeFeatureGroupRequest,
  DescribeFeatureGroupResponse,
  FeatureDefinition,
  FeatureType
}
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.SageMakerFeatureStoreRuntimeClient
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.{FeatureValue, PutRecordRequest}
import software.amazon.sagemaker.featurestore.sparksdk.exceptions.ValidationError
import software.amazon.sagemaker.featurestore.sparksdk.helpers.{
  ClientFactory,
  DataFrameRepartitioner,
  FeatureGroupArnResolver,
  SparkSessionInitializer
}

import java.util
import scala.collection.mutable.ListBuffer

class FeatureStoreManager extends Serializable {

  val SPARK_TYPE_TO_FEATURE_TYPE_MAP: Map[DataType, FeatureType] = Map(
    StringType  -> FeatureType.STRING,
    DoubleType  -> FeatureType.FRACTIONAL,
    FloatType   -> FeatureType.FRACTIONAL,
    ByteType    -> FeatureType.INTEGRAL,
    ShortType   -> FeatureType.INTEGRAL,
    IntegerType -> FeatureType.INTEGRAL,
    LongType    -> FeatureType.INTEGRAL
  )

  /** Batch ingest data into SageMaker FeatureStore.
   *
   *  @param inputDataFrame
   *    input Spark DataFrame to be ingested.
   *  @param featureGroupArn
   *    arn of a feature group.
   *  @param directOfflineStore
   *    choose if data should be only ingested to OfflineStore of a FeatureGroup.
   */
  def ingestData(inputDataFrame: DataFrame, featureGroupArn: String, directOfflineStore: Boolean = false): Unit = {
    SparkSessionInitializer.initializeSparkSession(inputDataFrame.sparkSession)

    val featureGroupArnResolver = new FeatureGroupArnResolver(featureGroupArn)
    val featureGroupName        = featureGroupArnResolver.resolveFeatureGroupName()
    val region                  = featureGroupArnResolver.resolveRegion()

    val describeResponse = getFeatureGroup(featureGroupName)
    validateDataFrameSchemaWithDescription(inputDataFrame, featureGroupArn, describeResponse)

    checkDirectOfflineStore(describeResponse, directOfflineStore)

    val eventTimeFeatureName = describeResponse.eventTimeFeatureName()
    val transformedDataFrame = transformDataFrameType(inputDataFrame, describeResponse)

    if (directOfflineStore || !isFeatureGroupOnlineStoreEnabled(describeResponse)) {
      batchIngestIntoOfflineStore(
        transformedDataFrame,
        describeResponse,
        eventTimeFeatureName,
        region
      )
    } else {
      streamIngestIntoOnlineStore(featureGroupName, transformedDataFrame)
    }
  }

  /** Load feature definitions according to the schema of input data frame.
   *
   *  @param inputDataFrame
   *    input Spark DataFrame to be loaded.
   *  @return
   *    list of feature definitions.
   */
  def loadFeatureDefinitionsFromSchema(inputDataFrame: DataFrame): util.List[FeatureDefinition] = {
    val fields = inputDataFrame.schema.fields
    val featureDefinitions: List[FeatureDefinition] = fields.foldLeft(List[FeatureDefinition]()) {
      (resultList, field) =>
        SPARK_TYPE_TO_FEATURE_TYPE_MAP.get(field.dataType) match {
          case Some(featureType) =>
            resultList :+ FeatureDefinition
              .builder()
              .featureName(field.name)
              .featureType(featureType)
              .build()
          case None =>
            throw ValidationError(
              f"Found unsupported data type from schema '${field.dataType}' which cannot be converted to a corresponding feature type."
            )
        }
    }
    featureDefinitions.asJava
  }

  /** Validate data against SageMaker Feature Group schema.
   *  @param inputDataFrame
   *    input Spark DataFrame to be validated.
   *  @param featureGroupArn
   *    arn of a Feature Group.
   */
  def validateDataFrameSchema(inputDataFrame: DataFrame, featureGroupArn: String): Unit = {
    val featureGroupArnResolver = new FeatureGroupArnResolver(featureGroupArn)
    val featureGroupName        = featureGroupArnResolver.resolveFeatureGroupName()

    val describeResponse = getFeatureGroup(featureGroupName)

    validateDataFrameSchemaWithDescription(inputDataFrame, featureGroupArn, describeResponse)
  }

  private def validateDataFrameSchemaWithDescription(
      inputDataFrame: DataFrame,
      featureGroupArn: String,
      featureGroupDescription: DescribeFeatureGroupResponse
  ): Unit = {
    checkIfFeatureGroupArnIdentical(featureGroupDescription, featureGroupArn)
    checkIfFeatureGroupIsCreated(featureGroupDescription)

    validateInputDataFrame(inputDataFrame, featureGroupDescription)
  }

  private def streamIngestIntoOnlineStore(featureGroupName: String, inputDataFrame: DataFrame): Unit = {
    val columns                = inputDataFrame.schema.names
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
    rows.foreach(row => {
      val record = ListBuffer[FeatureValue]()
      columns.foreach(columnName => {
        try {
          if (!row.isNullAt(row.fieldIndex(columnName))) {
            val featureValue = row.getAs[Any](columnName)
            record += FeatureValue
              .builder()
              .featureName(columnName)
              .valueAsString(featureValue.toString)
              .build()
          }
        } catch {
          case e: Throwable => throw new RuntimeException(e)
        }
      })
      try {
        val putRecordRequest = PutRecordRequest
          .builder()
          .featureGroupName(featureGroupName)
          .record(record.asJava)
          .build()
        runTimeClient.putRecord(putRecordRequest)
      } catch {
        case e: Throwable => throw new RuntimeException(e)
      }
    })
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
    val tempDataFrame = dataFrame
      .withColumn("temp_event_time_col", col(eventTimeFeatureName).cast("Timestamp"))
      .withColumn("year", date_format(col("temp_event_time_col"), "yyyy"))
      .withColumn("month", date_format(col("temp_event_time_col"), "MM"))
      .withColumn("day", date_format(col("temp_event_time_col"), "dd"))
      .withColumn("hour", date_format(col("temp_event_time_col"), "HH"))
      .withColumn("api_invocation_time", current_timestamp())
      .withColumn("write_time", current_timestamp())
      .withColumn("is_deleted", lit(false))
      .drop("temp_event_time_col")

    tempDataFrame
      .repartition(col("year"), col("month"), col("day"), col("hour"))
      .write
      .partitionBy("year", "month", "day", "hour")
      .option("compression", "none")
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
