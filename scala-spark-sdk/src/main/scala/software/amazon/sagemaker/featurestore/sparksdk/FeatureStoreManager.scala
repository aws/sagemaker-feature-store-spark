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

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import software.amazon.sagemaker.featurestore.sparksdk.helpers.FeatureGroupHelper._
import software.amazon.sagemaker.featurestore.sparksdk.validators.InputDataSchemaValidator._
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, lit, trunc}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{
  ByteType,
  DataType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  ShortType,
  StringType,
  StructField,
  StructType
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
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.{FeatureValue, PutRecordRequest, TargetStore}
import software.amazon.sagemaker.featurestore.sparksdk.exceptions.{StreamIngestionFailureException, ValidationError}
import software.amazon.sagemaker.featurestore.sparksdk.helpers.{
  ClientFactory,
  DataFrameRepartitioner,
  FeatureGroupArnResolver,
  SparkSessionInitializer
}

import java.util
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class FeatureStoreManager(assumeRoleArn: String = null) extends Serializable {

  val SPARK_TYPE_TO_FEATURE_TYPE_MAP: Map[DataType, FeatureType] = Map(
    StringType  -> FeatureType.STRING,
    DoubleType  -> FeatureType.FRACTIONAL,
    FloatType   -> FeatureType.FRACTIONAL,
    ByteType    -> FeatureType.INTEGRAL,
    ShortType   -> FeatureType.INTEGRAL,
    IntegerType -> FeatureType.INTEGRAL,
    LongType    -> FeatureType.INTEGRAL
  )

  private val ONLINE_INGESTION_ERROR_FILED_NAME: String = "online_ingestion_error"

  private var failedStreamIngestionDataFrame: Option[DataFrame] = None

  private var dataFrameSizeCounter: LongAccumulator = null

  /** Batch ingest data into SageMaker FeatureStore.
   *
   *  @param inputDataFrame
   *    input Spark DataFrame to be ingested.
   *  @param featureGroupArn
   *    arn of a feature group.
   *  @param targetStores
   *    choose the target store to ingest the data
   */
  def ingestData(inputDataFrame: DataFrame, featureGroupArn: String, targetStores: List[String] = null): Unit = {

    initializeAccumulators(inputDataFrame.sparkSession.sparkContext)

    val featureGroupArnResolver = new FeatureGroupArnResolver(featureGroupArn)
    val featureGroupName        = featureGroupArn
    val region                  = featureGroupArnResolver.resolveRegion()

    ClientFactory.initialize(region = region, roleArn = assumeRoleArn)

    val describeResponse = getFeatureGroup(featureGroupName)

    checkIfFeatureGroupIsCreated(describeResponse)
    val parsedTargetStores = checkAndParseTargetStore(describeResponse, targetStores)

    val eventTimeFeatureName = describeResponse.eventTimeFeatureName()
    val recordIdentifierName = describeResponse.recordIdentifierFeatureName()

    if (parsedTargetStores == null || shouldIngestInStream(parsedTargetStores)) {
      validateSchemaNames(inputDataFrame.schema.names, describeResponse, recordIdentifierName, eventTimeFeatureName)
      streamIngestIntoOnlineStore(featureGroupName, inputDataFrame, parsedTargetStores, region)
    } else {

      val validatedInputDataFrame = validateInputDataFrame(inputDataFrame, describeResponse)

      batchIngestIntoOfflineStore(
        validatedInputDataFrame,
        describeResponse,
        eventTimeFeatureName,
        region
      )
    }
  }

  def ingestDataInJava(
      inputDataFrame: org.apache.spark.sql.Dataset[Row],
      featureGroupArn: java.lang.String,
      targetStores: java.util.ArrayList[String] = null
  ): Unit = {
    ingestData(inputDataFrame, featureGroupArn, if (targetStores != null) targetStores.asScala.toList else null)
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

  /** Get the dataframe which contains failed records during last online ingestion
   *
   *  @return
   *    dataframe which contains records failed to be ingested
   */
  def getFailedStreamIngestionDataFrame: DataFrame = {
    failedStreamIngestionDataFrame.orNull
  }

  private def streamIngestIntoOnlineStore(
      featureGroupName: String,
      inputDataFrame: DataFrame,
      targetStores: List[TargetStore],
      region: String
  ): Unit = {
    val columns                = inputDataFrame.schema.names
    val repartitionedDataFrame = DataFrameRepartitioner.repartition(inputDataFrame)

    // Add extra field for reporting online ingestion failures
    val castWithExceptionSchema = StructType(
      repartitionedDataFrame.schema.fields ++ Array(StructField(ONLINE_INGESTION_ERROR_FILED_NAME, StringType, true))
    )
    val fieldIndexMap = castWithExceptionSchema.fieldNames.zipWithIndex.toMap

    // Encoder needs to be defined during transformation because the original schema is changed.
    // The dataframe has to be cached otherwise the input dataset will be re-ingested when customer perform spark
    // actions on failedStreamIngestionDataFrame.
    failedStreamIngestionDataFrame = Option(
      repartitionedDataFrame
        .mapPartitions(partition => {
          ClientFactory.initialize(region, assumeRoleArn)

          putOnlineRecordsForPartition(
            partition,
            featureGroupName,
            columns,
            targetStores,
            ClientFactory.sageMakerFeatureStoreRuntimeClientBuilder.build()
          )
        })(RowEncoder(castWithExceptionSchema))
        .filter(row => row.getAs[String](fieldIndexMap(ONLINE_INGESTION_ERROR_FILED_NAME)) != null)
        .cache()
    )

    // MapPartitions and Map are lazily evaluated by spark, so action is needed here to ensure ingestion is executed
    // For more info: https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions
    val failedOnlineIngestionDataFrameSize     = failedStreamIngestionDataFrame.get.count()
    val successfulOnlineIngestionDataFrameSize = dataFrameSizeCounter.value - failedOnlineIngestionDataFrameSize

    if (successfulOnlineIngestionDataFrameSize > 0) {
      println(s"Stream ingestion finished, ingested ${successfulOnlineIngestionDataFrameSize} records")
    }

    if (failedOnlineIngestionDataFrameSize > 0) {
      throw StreamIngestionFailureException(
        s"Stream ingestion finished, however ${failedOnlineIngestionDataFrameSize} records failed to be ingested. Please inspect failed stream ingestion data frame for more info."
      )
    }
  }

  private def putOnlineRecordsForPartition(
      partition: Iterator[Row],
      featureGroupName: String,
      columns: Array[String],
      targetStores: List[TargetStore],
      runTimeClient: SageMakerFeatureStoreRuntimeClient
  ): Iterator[Row] = {
    val newPartition = partition.map(row => {
      // Increment the row counter value
      dataFrameSizeCounter.add(1)
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

      val errorMessage = Try {
        val putRecordRequestBuilder = PutRecordRequest
          .builder()
          .featureGroupName(featureGroupName)
          .record(record.asJava)

        if (targetStores != null) {
          putRecordRequestBuilder.targetStores(targetStores.asJava)
        }
        runTimeClient.putRecord(putRecordRequestBuilder.build())
      } match {
        case Success(value) => null
        case Failure(ex)    => ex.getMessage
      }

      Row.fromSeq(row.toSeq.toList :+ errorMessage)
    })

    newPartition
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
    val tableFormat         = describeResponse.offlineStoreConfig().tableFormat()
    val destinationFilePath = generateDestinationFilePath(describeResponse)
    val tempDataFrame = dataFrame
      .withColumn("api_invocation_time", current_timestamp())
      .withColumn("write_time", current_timestamp())
      .withColumn("is_deleted", lit(false))

    if (isIcebergTableEnabled(describeResponse)) {
      val resolvedOutputS3Uri = describeResponse.offlineStoreConfig().s3StorageConfig().resolvedOutputS3Uri()
      val dataCatalogName     = describeResponse.offlineStoreConfig().dataCatalogConfig().catalog().toLowerCase()
      val dataBaseName        = describeResponse.offlineStoreConfig().dataCatalogConfig().database().toLowerCase()
      val tableName           = describeResponse.offlineStoreConfig().dataCatalogConfig().tableName().toLowerCase()

      SparkSessionInitializer.initializeSparkSessionForIcebergTable(
        dataFrame.sparkSession,
        offlineStoreEncryptionKeyId,
        resolvedOutputS3Uri,
        dataCatalogName,
        assumeRoleArn,
        region
      )

      tempDataFrame
        .sortWithinPartitions(col(eventTimeFeatureName))
        .writeTo(f"$dataCatalogName.$dataBaseName.`$tableName`")
        .option("compression", "none")
        .append()
    } else if (isGlueTableEnabled(describeResponse) || tableFormat == null) {
      SparkSessionInitializer.initializeSparkSessionForOfflineStore(
        dataFrame.sparkSession,
        offlineStoreEncryptionKeyId,
        assumeRoleArn,
        region
      )

      val offlineDataFrame = tempDataFrame
        .withColumn("temp_event_time_col", col(eventTimeFeatureName).cast("Timestamp"))
        .withColumn("year", date_format(col("temp_event_time_col"), "yyyy"))
        .withColumn("month", date_format(col("temp_event_time_col"), "MM"))
        .withColumn("day", date_format(col("temp_event_time_col"), "dd"))
        .withColumn("hour", date_format(col("temp_event_time_col"), "HH"))
        .drop("temp_event_time_col")

      offlineDataFrame
        .repartition(col("year"), col("month"), col("day"), col("hour"))
        .write
        .partitionBy("year", "month", "day", "hour")
        .option("compression", "none")
        .mode("append")
        .parquet(destinationFilePath)
    } else {
      val tableFormat = describeResponse.offlineStoreConfig().tableFormat()
      throw new RuntimeException(
        f"Invalid table format '$tableFormat' detected and is not supported by feature store spark connector."
      )
    }
  }

  private def getFeatureGroup(featureGroupName: String): DescribeFeatureGroupResponse = {
    val describeRequest = DescribeFeatureGroupRequest
      .builder()
      .featureGroupName(featureGroupName)
      .build()
    ClientFactory.sageMakerClient.describeFeatureGroup(describeRequest)
  }

  private def shouldIngestInStream(targetStores: List[TargetStore]): Boolean = {
    targetStores.contains(TargetStore.ONLINE_STORE)
  }

  private def initializeAccumulators(sparkConext: SparkContext): Unit = {
    dataFrameSizeCounter = sparkConext.longAccumulator("dataFrameSizeCounter")
  }
}
