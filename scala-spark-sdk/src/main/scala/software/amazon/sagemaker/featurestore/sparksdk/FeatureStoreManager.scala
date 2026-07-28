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
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.{
  BatchWriteRecordEntry,
  BatchWriteRecordRequest,
  FeatureValue,
  ListRecordsRequest,
  PutRecordRequest,
  TargetStore
}
import org.slf4j.LoggerFactory
import software.amazon.sagemaker.featurestore.sparksdk.exceptions.{StreamIngestionFailureException, ValidationError}
import software.amazon.sagemaker.featurestore.sparksdk.helpers.{
  ClientFactory,
  DataFrameRepartitioner,
  FeatureGroupArnResolver,
  LakeFormationCredentials,
  LakeFormationHelper,
  MinSparkVersionGate,
  SparkSessionInitializer
}

import java.util
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class FeatureStoreManager(assumeRoleArn: String = null) extends Serializable {

  private val logger = LoggerFactory.getLogger(this.getClass)

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
   *  @param useLakeFormationCredentials
   *    whether to use LakeFormation for offline store ingestion (default: false)
   *  @param useBatchWriteRecord
   *    whether to use BatchWriteRecord API (25 records per call) instead of PutRecord (1 record per call) for online
   *    store ingestion. Requires both sagemaker:BatchWriteRecord AND sagemaker:PutRecord IAM permissions. (default:
   *    false)
   */
  def ingestData(
      inputDataFrame: DataFrame,
      featureGroupArn: String,
      targetStores: List[String] = null,
      useLakeFormationCredentials: Boolean = false,
      useBatchWriteRecord: Boolean = false
  ): Unit = {

    logger.info(
      s"ingestData: featureGroupArn=$featureGroupArn, targetStores=$targetStores, useLakeFormationCredentials=$useLakeFormationCredentials, useBatchWriteRecord=$useBatchWriteRecord"
    )

    initializeAccumulators(inputDataFrame.sparkSession.sparkContext)

    val featureGroupArnResolver = new FeatureGroupArnResolver(featureGroupArn)
    val featureGroupName        = featureGroupArn
    val region                  = featureGroupArnResolver.resolveRegion()
    val accountId               = featureGroupArnResolver.resolveAccountId()

    ClientFactory.initialize(region = region, roleArn = assumeRoleArn)

    val describeResponse = getFeatureGroup(featureGroupName)

    checkIfFeatureGroupIsCreated(describeResponse)
    val parsedTargetStores = checkAndParseTargetStore(describeResponse, targetStores)

    val eventTimeFeatureName = describeResponse.eventTimeFeatureName()
    val recordIdentifierName = describeResponse.recordIdentifierFeatureName()

    if (parsedTargetStores == null || shouldIngestInStream(parsedTargetStores)) {
      validateSchemaNames(inputDataFrame.schema.names, describeResponse, recordIdentifierName, eventTimeFeatureName)
      streamIngestIntoOnlineStore(featureGroupName, inputDataFrame, parsedTargetStores, region, useBatchWriteRecord)
    } else {

      val validatedInputDataFrame = validateInputDataFrame(inputDataFrame, describeResponse)

      batchIngestIntoOfflineStore(
        validatedInputDataFrame,
        describeResponse,
        eventTimeFeatureName,
        region,
        accountId,
        useLakeFormationCredentials
      )
    }
  }

  def ingestDataInJava(
      inputDataFrame: org.apache.spark.sql.Dataset[Row],
      featureGroupArn: java.lang.String,
      targetStores: java.util.ArrayList[String] = null,
      useLakeFormationCredentials: java.lang.Boolean = false,
      useBatchWriteRecord: java.lang.Boolean = false
  ): Unit = {
    ingestData(
      inputDataFrame,
      featureGroupArn,
      if (targetStores != null) targetStores.asScala.toList else null,
      Option(useLakeFormationCredentials).map(_.booleanValue()).getOrElse(false),
      Option(useBatchWriteRecord).map(_.booleanValue()).getOrElse(false)
    )
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

  /** List record identifiers from a FeatureGroup's OnlineStore (single page).
   *
   *  @param featureGroupArn
   *    ARN or name of the feature group
   *  @param maxResults
   *    maximum number of record identifiers to return (1-100)
   *  @param nextToken
   *    pagination token from a previous response
   *  @param includeSoftDeletedRecords
   *    if true, include soft-deleted records
   *  @return
   *    tuple of (list of record identifier strings, nextToken or null)
   */
  def listRecords(
      featureGroupArn: String,
      maxResults: java.lang.Integer = null,
      nextToken: String = null,
      includeSoftDeletedRecords: Boolean = false
  ): java.util.Map[String, Object] = {
    val featureGroupName = featureGroupArn
    val region           = new FeatureGroupArnResolver(featureGroupArn).resolveRegion()
    ClientFactory.initialize(region = region, roleArn = assumeRoleArn)

    val requestBuilder = ListRecordsRequest
      .builder()
      .featureGroupName(featureGroupName)
      .includeSoftDeletedRecords(includeSoftDeletedRecords)

    if (maxResults != null) requestBuilder.maxResults(maxResults)
    if (nextToken != null) requestBuilder.nextToken(nextToken)

    val client   = ClientFactory.sageMakerFeatureStoreRuntimeClientBuilder.build()
    val response = client.listRecords(requestBuilder.build())

    val result = new java.util.HashMap[String, Object]()
    result.put("RecordIdentifiers", response.recordIdentifiers())
    result.put("NextToken", response.nextToken())
    result
  }

  private def streamIngestIntoOnlineStore(
      featureGroupName: String,
      inputDataFrame: DataFrame,
      targetStores: List[TargetStore],
      region: String,
      useBatchWriteRecord: Boolean = false
  ): Unit = {
    val columns                = inputDataFrame.schema.names
    val repartitionedDataFrame = DataFrameRepartitioner.repartition(inputDataFrame)

    // Add extra field for reporting online ingestion failures
    val castWithExceptionSchema = StructType(
      repartitionedDataFrame.schema.fields ++ Array(StructField(ONLINE_INGESTION_ERROR_FILED_NAME, StringType, true))
    )
    val fieldIndexMap = castWithExceptionSchema.fieldNames.zipWithIndex.toMap

    if (useBatchWriteRecord) {
      logger.info(
        s"Using BatchWriteRecord API (batch_size=25). " +
          s"Requires sagemaker:BatchWriteRecord AND sagemaker:PutRecord IAM permissions."
      )
    }

    // Encoder needs to be defined during transformation because the original schema is changed.
    // The dataframe has to be cached otherwise the input dataset will be re-ingested when customer perform spark
    // actions on failedStreamIngestionDataFrame.
    failedStreamIngestionDataFrame = Option(
      repartitionedDataFrame
        .mapPartitions(partition => {
          ClientFactory.initialize(region, assumeRoleArn)

          if (useBatchWriteRecord) {
            batchWriteOnlineRecordsForPartition(
              partition,
              featureGroupName,
              columns,
              targetStores,
              ClientFactory.sageMakerFeatureStoreRuntimeClientBuilder.build()
            )
          } else {
            putOnlineRecordsForPartition(
              partition,
              featureGroupName,
              columns,
              targetStores,
              ClientFactory.sageMakerFeatureStoreRuntimeClientBuilder.build()
            )
          }
        })(SparkRowEncoderAdaptor.encoderFor(castWithExceptionSchema))
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

  private val BATCH_WRITE_MAX_ENTRIES = 25

  private def batchWriteOnlineRecordsForPartition(
      partition: Iterator[Row],
      featureGroupName: String,
      columns: Array[String],
      targetStores: List[TargetStore],
      runTimeClient: SageMakerFeatureStoreRuntimeClient
  ): Iterator[Row] = {
    val results = ListBuffer[Row]()

    partition.grouped(BATCH_WRITE_MAX_ENTRIES).foreach { batch =>
      val batchRows = batch.toList
      val entries   = ListBuffer[BatchWriteRecordEntry]()

      batchRows.foreach { row =>
        val record = ListBuffer[FeatureValue]()
        columns.foreach { columnName =>
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
        }

        val entryBuilder = BatchWriteRecordEntry
          .builder()
          .featureGroupName(featureGroupName)
          .record(record.asJava)

        if (targetStores != null) {
          entryBuilder.targetStores(targetStores.asJava)
        }

        entries += entryBuilder.build()
      }

      Try {
        val request = BatchWriteRecordRequest
          .builder()
          .entries(entries.asJava)
          .build()
        runTimeClient.batchWriteRecord(request)
      } match {
        case Success(response) =>
          // Build a set of failed entry indices by matching error.entry() and unprocessedEntries
          val failedEntryIndices = scala.collection.mutable.Set[Int]()

          // Map errors back to specific entries
          val errorEntries = response.errors().asScala
          errorEntries.foreach { error =>
            val failedEntry = error.entry()
            val idx         = entries.indexOf(failedEntry)
            if (idx >= 0) {
              failedEntryIndices += idx
            } else {
              logger.warn(s"Could not match error entry back to original batch: $failedEntry")
            }
          }

          // Map unprocessed entries back to specific entries
          val unprocessed = response.unprocessedEntries().asScala
          unprocessed.foreach { entry =>
            val idx = entries.indexOf(entry)
            if (idx >= 0) {
              failedEntryIndices += idx
            } else {
              logger.warn(s"Could not match unprocessed entry back to original batch: $entry")
            }
          }

          // Mark only failed rows with error, successful rows get null
          batchRows.zipWithIndex.foreach { case (row, idx) =>
            val errorMessage = if (failedEntryIndices.contains(idx)) {
              val errorDetail = errorEntries
                .find(e => entries.indexOf(e.entry()) == idx)
                .map(e => s"${e.errorCode()}: ${e.errorMessage()}")
                .getOrElse("Unprocessed entry")
              errorDetail
            } else {
              null
            }
            results += Row.fromSeq(row.toSeq.toList :+ errorMessage)
          }

        case Failure(ex) =>
          // Entire batch failed — mark all rows with the exception message
          batchRows.foreach { row =>
            results += Row.fromSeq(row.toSeq.toList :+ ex.getMessage)
          }
      }
    }

    results.iterator
  }

  private def batchIngestIntoOfflineStore(
      dataFrame: DataFrame,
      describeResponse: DescribeFeatureGroupResponse,
      eventTimeFeatureName: String,
      region: String,
      accountId: String,
      useLakeFormationCredentials: Boolean = false
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

    val lfCredentials: Option[LakeFormationCredentials] = if (useLakeFormationCredentials) {
      // Build-time gate: on Spark <3.5 builds requireSparkVersion(3, 5) throws; on Spark 3.5+ it is a no-op.
      MinSparkVersionGate.requireSparkVersion(3, 5)
      val dataCatalogConfig = describeResponse.offlineStoreConfig().dataCatalogConfig()
      logger.info(s"dataCatalogConfig=${if (dataCatalogConfig != null)
        s"database=${dataCatalogConfig.database()}, table=${dataCatalogConfig.tableName()}"
      else "null"}")
      if (dataCatalogConfig == null) {
        throw new RuntimeException(
          "Lake Formation credential vending requires a Data Catalog configuration on the feature group's offline store."
        )
      }
      val database  = dataCatalogConfig.database().toLowerCase()
      val tableName = dataCatalogConfig.tableName().toLowerCase()
      val partition = new FeatureGroupArnResolver(describeResponse.featureGroupArn()).resolvePartition()
      Some(LakeFormationHelper.vendCredentials(region, accountId, partition, database, tableName))
    } else {
      logger.info("LakeFormation credential vending disabled by caller")
      None
    }

    val resolvedOutputS3Uri = describeResponse.offlineStoreConfig().s3StorageConfig().resolvedOutputS3Uri()

    val tempDataFrame = dataFrame
      .withColumn("api_invocation_time", current_timestamp())
      .withColumn("write_time", current_timestamp())
      .withColumn("is_deleted", lit(false))

    if (isIcebergTableEnabled(describeResponse)) {
      val dataCatalogName = describeResponse.offlineStoreConfig().dataCatalogConfig().catalog().toLowerCase()
      val dataBaseName    = describeResponse.offlineStoreConfig().dataCatalogConfig().database().toLowerCase()
      val tableName       = describeResponse.offlineStoreConfig().dataCatalogConfig().tableName().toLowerCase()

      // Refresh LF credentials just before the write to minimize the window between vending and
      // the actual S3 write. Note: for very large DataFrames the 1-hour credential window may
      // still expire mid-write. Users with long-running ingestion jobs should partition their
      // data into smaller batches.
      val refreshedLfCredentials = lfCredentials.map(LakeFormationHelper.refreshIfNeeded)

      SparkSessionInitializer.initializeSparkSessionForIcebergTable(
        dataFrame.sparkSession,
        offlineStoreEncryptionKeyId,
        resolvedOutputS3Uri,
        dataCatalogName,
        assumeRoleArn,
        region,
        refreshedLfCredentials
      )

      tempDataFrame
        .sortWithinPartitions(col(eventTimeFeatureName))
        .writeTo(f"$dataCatalogName.$dataBaseName.`$tableName`")
        .option("compression", "none")
        .append()
    } else if (isGlueTableEnabled(describeResponse) || tableFormat == null) {
      // Refresh LF credentials just before the write to minimize the window between vending and
      // the actual S3 write. Note: for very large DataFrames the 1-hour credential window may
      // still expire mid-write. Users with long-running ingestion jobs should partition their
      // data into smaller batches.
      val refreshedLfCredentials = lfCredentials.map(LakeFormationHelper.refreshIfNeeded)

      SparkSessionInitializer.initializeSparkSessionForOfflineStore(
        dataFrame.sparkSession,
        offlineStoreEncryptionKeyId,
        assumeRoleArn,
        region,
        resolvedOutputS3Uri,
        refreshedLfCredentials
      )

      // LF-vended creds scope S3 access to objects UNDER the registered prefix. The S3A
      // committer's setupJob mkdirs probes the prefix: LIST first (empty on a fresh feature
      // group), then HEAD on the prefix-as-object (LF denies => 403). Seed a marker object so
      // the LIST is non-empty and the HEAD is skipped. No-op when LF is not in use.
      refreshedLfCredentials.foreach { _ =>
        LakeFormationHelper.seedLfPrefix(dataFrame.sparkSession, resolvedOutputS3Uri)
      }

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
