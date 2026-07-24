package software.amazon.sagemaker.featurestore.sparksdk

import collection.JavaConverters._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.doNothing
import org.mockito.MockitoSugar.{times, verify, when, withObjectMocked}
import org.mockito.captor.ArgCaptor
import org.scalatest.Matchers.convertToAnyShouldWrapper
import org.scalatest.PrivateMethodTester
import org.scalatestplus.mockito.MockitoSugar.mock
import org.scalatestplus.testng.TestNGSuite
import org.testng.Assert.assertEquals
import org.testng.annotations.{AfterTest, BeforeMethod, DataProvider, Test}
import software.amazon.awssdk.services.sagemaker.SageMakerClient
import software.amazon.awssdk.services.sagemaker.model.{
  DataCatalogConfig,
  DescribeFeatureGroupRequest,
  DescribeFeatureGroupResponse,
  FeatureDefinition,
  FeatureGroupStatus,
  FeatureType,
  OfflineStoreConfig,
  OnlineStoreConfig,
  S3StorageConfig,
  TableFormat
}
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.{
  BatchWriteRecordEntry,
  BatchWriteRecordError,
  BatchWriteRecordRequest,
  BatchWriteRecordResponse,
  FeatureValue,
  ListRecordsRequest,
  ListRecordsResponse,
  PutRecordRequest,
  PutRecordResponse,
  TargetStore
}
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.{
  SageMakerFeatureStoreRuntimeClient,
  SageMakerFeatureStoreRuntimeClientBuilder
}
import software.amazon.sagemaker.featurestore.sparksdk.exceptions.{StreamIngestionFailureException, ValidationError}
import software.amazon.sagemaker.featurestore.sparksdk.helpers.{ClientFactory, SparkSessionInitializer}

import java.io.File
import scala.reflect.io.Directory

class FeatureStoreManagerTest extends TestNGSuite with PrivateMethodTester {

  private final val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("TestProgram")
    .master("local")
    .config("spark.sql.catalogImplementation", "in-memory")
    .getOrCreate()
  import sparkSession.implicits._

  private final val TEST_FEATURE_GROUP_ARN = "arn:aws:sagemaker:us-west-2:123456789012:feature-group/test-feature-group"
  private final val TEST_ARTIFACT_ROOT     = "./test-artifact"

  private final val featureStoreManager = new FeatureStoreManager()

  private final val mockedSageMakerFeatureStoreRuntimeClientBuilder = mock[SageMakerFeatureStoreRuntimeClientBuilder]
  private final val mockedSageMakerClient                           = mock[SageMakerClient]
  private final val mockedSageMakerFeatureStoreRuntimeClient        = mock[SageMakerFeatureStoreRuntimeClient]
  private final val putRecordRequestCaptor                          = ArgCaptor[PutRecordRequest]

  @BeforeMethod
  def setup(): Unit = {
    ClientFactory.skipInitialization = true
    ClientFactory.sageMakerClient = mockedSageMakerClient
    ClientFactory.sageMakerFeatureStoreRuntimeClientBuilder = mockedSageMakerFeatureStoreRuntimeClientBuilder

    org.mockito.Mockito.reset(mockedSageMakerFeatureStoreRuntimeClient)
    org.mockito.Mockito.reset(mockedSageMakerClient)

    when(mockedSageMakerFeatureStoreRuntimeClientBuilder.build()).thenReturn(mockedSageMakerFeatureStoreRuntimeClient)
    when(mockedSageMakerFeatureStoreRuntimeClient.putRecord(any(classOf[PutRecordRequest])))
      .thenReturn(PutRecordResponse.builder().build())
    when(mockedSageMakerFeatureStoreRuntimeClient.batchWriteRecord(any(classOf[BatchWriteRecordRequest])))
      .thenReturn(
        BatchWriteRecordResponse
          .builder()
          .errors(java.util.Collections.emptyList[BatchWriteRecordError]())
          .unprocessedEntries(java.util.Collections.emptyList[BatchWriteRecordEntry]())
          .build()
      )
    when(mockedSageMakerFeatureStoreRuntimeClient.listRecords(any(classOf[ListRecordsRequest])))
      .thenReturn(
        ListRecordsResponse
          .builder()
          .recordIdentifiers(java.util.Arrays.asList("id-1", "id-2"))
          .build()
      )
  }

  @Test(dataProvider = "ingestDataStreamOnlineStoreTestDataProvider")
  def ingestDataStreamOnlineStoreTest(
      inputDataFrame: DataFrame,
      expectedPutRecordRequest: PutRecordRequest,
      inputTargetStores: List[String]
  ): Unit = {
    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupArn(TEST_FEATURE_GROUP_ARN)
      .featureGroupStatus(FeatureGroupStatus.CREATED)
      .eventTimeFeatureName("event-time")
      .recordIdentifierFeatureName("record-identifier")
      .featureDefinitions(
        FeatureDefinition
          .builder()
          .featureName("record-identifier")
          .featureType(FeatureType.STRING)
          .build(),
        FeatureDefinition
          .builder()
          .featureName("event-time")
          .featureType(FeatureType.STRING)
          .build()
      )
      .onlineStoreConfig(
        OnlineStoreConfig
          .builder()
          .enableOnlineStore(true)
          .build()
      )
      .build()

    when(mockedSageMakerClient.describeFeatureGroup(any(classOf[DescribeFeatureGroupRequest]))).thenReturn(response)

    featureStoreManager.ingestData(
      inputDataFrame,
      TEST_FEATURE_GROUP_ARN,
      inputTargetStores
    )
    verify(mockedSageMakerFeatureStoreRuntimeClient).putRecord(
      putRecordRequestCaptor
    )
    putRecordRequestCaptor.hasCaptured(expectedPutRecordRequest)

    val failedStreamIngestionDataFrame = featureStoreManager.getFailedStreamIngestionDataFrame

    assertEquals(failedStreamIngestionDataFrame.count(), 0)
  }

  @Test(dataProvider = "ingestDataStreamOnlineStoreTestDataProvider")
  def ingestDataStreamOnlineStoreWithFailuresTest(
      inputDataFrame: DataFrame,
      expectedPutRecordRequest: PutRecordRequest,
      inputTargetStores: List[String]
  ): Unit = {
    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupArn(TEST_FEATURE_GROUP_ARN)
      .featureGroupStatus(FeatureGroupStatus.CREATED)
      .eventTimeFeatureName("event-time")
      .recordIdentifierFeatureName("record-identifier")
      .featureDefinitions(
        FeatureDefinition
          .builder()
          .featureName("record-identifier")
          .featureType(FeatureType.STRING)
          .build(),
        FeatureDefinition
          .builder()
          .featureName("event-time")
          .featureType(FeatureType.STRING)
          .build()
      )
      .onlineStoreConfig(
        OnlineStoreConfig
          .builder()
          .enableOnlineStore(true)
          .build()
      )
      .build()

    when(mockedSageMakerFeatureStoreRuntimeClient.putRecord(any(classOf[PutRecordRequest])))
      .thenThrow(new RuntimeException("test error"))
    when(mockedSageMakerClient.describeFeatureGroup(any(classOf[DescribeFeatureGroupRequest]))).thenReturn(response)

    ClientFactory.sageMakerClient = mockedSageMakerClient
    ClientFactory.sageMakerFeatureStoreRuntimeClientBuilder = mockedSageMakerFeatureStoreRuntimeClientBuilder

    val caught = intercept[StreamIngestionFailureException] {
      featureStoreManager.ingestData(
        inputDataFrame,
        TEST_FEATURE_GROUP_ARN,
        inputTargetStores
      )
    }

    caught.message shouldBe "Stream ingestion finished, however 1 records failed to be ingested. Please inspect failed stream ingestion data frame for more info."

    val failedStreamIngestionDataFrame = featureStoreManager.getFailedStreamIngestionDataFrame

    assertEquals(failedStreamIngestionDataFrame.count(), inputDataFrame.count())
    assertEquals(failedStreamIngestionDataFrame.first().getAs[String]("online_ingestion_error"), "test error")
  }

  @Test(dataProvider = "ingestDataBatchOfflineStoreGlueTableTestDataProvider")
  def ingestDataBatchOfflineStoreGlueTableTest(
      inputDataFrame: DataFrame,
      putRecordRequest: PutRecordRequest,
      targetStores: List[String],
      tableFormat: TableFormat
  ): Unit = {
    val resolvedOutputPath =
      TEST_ARTIFACT_ROOT + "/ingest-data-direct-offline-store-test/only-offline-store-enabled"
    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupArn(TEST_FEATURE_GROUP_ARN)
      .featureGroupStatus(FeatureGroupStatus.CREATED)
      .eventTimeFeatureName("event-time")
      .recordIdentifierFeatureName("record-identifier")
      .featureDefinitions(
        FeatureDefinition
          .builder()
          .featureName("record-identifier")
          .featureType(FeatureType.STRING)
          .build(),
        FeatureDefinition
          .builder()
          .featureName("event-time")
          .featureType(FeatureType.STRING)
          .build()
      )
      .offlineStoreConfig(
        OfflineStoreConfig
          .builder()
          .tableFormat(tableFormat)
          .s3StorageConfig(
            S3StorageConfig
              .builder()
              .resolvedOutputS3Uri(resolvedOutputPath)
              .build()
          )
          .build()
      )
      .build()
    when(
      mockedSageMakerClient
        .describeFeatureGroup(any(classOf[DescribeFeatureGroupRequest]))
    ).thenReturn(response)

    withObjectMocked[ClientFactory.type] {
      when(ClientFactory.sageMakerClient).thenReturn(mockedSageMakerClient)

      doNothing().when(ClientFactory).initialize(anyString(), anyString())

      featureStoreManager.ingestData(
        inputDataFrame,
        TEST_FEATURE_GROUP_ARN,
        targetStores
      )
    }

    verify(mockedSageMakerFeatureStoreRuntimeClient, times(0))
      .putRecord(putRecordRequest)
    verifyDataIngestedInOfflineStore(inputDataFrame, resolvedOutputPath)
  }

  @Test
  def ingestDataBatchOfflineStoreIcebergTableTest(): Unit = {

    val resolvedOutputPath = TEST_ARTIFACT_ROOT + "/iceberg-table-ingestion"

    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupArn(TEST_FEATURE_GROUP_ARN)
      .featureGroupStatus(FeatureGroupStatus.CREATED)
      .eventTimeFeatureName("event_time")
      .recordIdentifierFeatureName("record_identifier")
      .featureDefinitions(
        FeatureDefinition
          .builder()
          .featureName("record_identifier")
          .featureType(FeatureType.STRING)
          .build(),
        FeatureDefinition
          .builder()
          .featureName("event_time")
          .featureType(FeatureType.STRING)
          .build()
      )
      .offlineStoreConfig(
        OfflineStoreConfig
          .builder()
          .tableFormat(TableFormat.ICEBERG)
          .dataCatalogConfig(
            DataCatalogConfig
              .builder()
              .catalog("local")
              .database("db")
              .tableName("table")
              .build()
          )
          .s3StorageConfig(
            S3StorageConfig
              .builder()
              .resolvedOutputS3Uri(resolvedOutputPath)
              .build()
          )
          .build()
      )
      .build()

    when(
      mockedSageMakerClient
        .describeFeatureGroup(any(classOf[DescribeFeatureGroupRequest]))
    ).thenReturn(response)

    val inputDataFrame = Seq(("identifier-1", "2021-05-06T05:12:14Z")).toDF("record_identifier", "event_time")

    withObjectMocked[SparkSessionInitializer.type] {
      doNothing()
        .when(SparkSessionInitializer)
        .initializeSparkSessionForIcebergTable(
          any(),
          anyString(),
          anyString(),
          anyString(),
          anyString(),
          anyString(),
          any()
        )

      // Create a local derby local database which enables to create local metastores for unit tests
      sparkSession.conf.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      sparkSession.conf.set("spark.sql.catalog.local.type", "hadoop")
      sparkSession.conf.set("spark.sql.catalog.local.warehouse", "test_warehouse")
      sparkSession.sql(
        "CREATE TABLE IF NOT EXISTS local.db.table (record_identifier string, event_time string, api_invocation_time timestamp, write_time timestamp, is_deleted boolean) USING iceberg PARTITIONED BY (truncate(event_time, 10))"
      )

      featureStoreManager.ingestData(
        inputDataFrame,
        TEST_FEATURE_GROUP_ARN,
        List("OfflineStore")
      )
      verifyDataIngestedInOfflineStore(inputDataFrame, "test_warehouse/db/table/data")
    }
    sparkSession.sql("DROP TABLE IF EXISTS local.db.table")
  }

  @Test(expectedExceptions = Array(classOf[RuntimeException]))
  def ingestDataBatchOfflineStoreInvalidTableFormatTest(): Unit = {
    val resolvedOutputPath =
      TEST_ARTIFACT_ROOT + "/ingest-data-direct-offline-store-test/only-offline-store-enabled"
    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupArn(TEST_FEATURE_GROUP_ARN)
      .featureGroupStatus(FeatureGroupStatus.CREATED)
      .eventTimeFeatureName("event-time")
      .recordIdentifierFeatureName("record-identifier")
      .featureDefinitions(
        FeatureDefinition
          .builder()
          .featureName("record-identifier")
          .featureType(FeatureType.STRING)
          .build(),
        FeatureDefinition
          .builder()
          .featureName("event-time")
          .featureType(FeatureType.STRING)
          .build()
      )
      .offlineStoreConfig(
        OfflineStoreConfig
          .builder()
          .tableFormat(TableFormat.UNKNOWN_TO_SDK_VERSION)
          .s3StorageConfig(
            S3StorageConfig
              .builder()
              .resolvedOutputS3Uri(resolvedOutputPath)
              .build()
          )
          .build()
      )
      .build()

    when(
      mockedSageMakerClient
        .describeFeatureGroup(any(classOf[DescribeFeatureGroupRequest]))
    ).thenReturn(response)

    withObjectMocked[ClientFactory.type] {
      when(ClientFactory.sageMakerClient).thenReturn(mockedSageMakerClient)

      doNothing().when(ClientFactory).initialize(anyString(), anyString())

      featureStoreManager.ingestData(
        Seq((123, 100.0, "dummy")).toDF("feature-integral", "feature-fractional", "feature-string"),
        TEST_FEATURE_GROUP_ARN,
        List("OfflineStore")
      )
    }
  }

  @Test(dataProvider = "loadFeatureDefinitionsFromSchemaTestDataProvider")
  def loadFeatureDefinitionsFromSchemaTest(
      inputDataFrame: DataFrame,
      expectedFeatureDefinitions: List[FeatureDefinition]
  ): Unit = {
    val featureDefinitions = featureStoreManager.loadFeatureDefinitionsFromSchema(inputDataFrame)
    assertEquals(featureDefinitions, expectedFeatureDefinitions.asJava)
  }

  @Test(
    dataProvider = "loadFeatureDefinitionsFromSchemaNegativeTestDataProvider",
    expectedExceptions = Array(classOf[ValidationError])
  )
  def loadFeatureDefinitionsFromSchemaTest_negative(
      inputDataFrame: DataFrame
  ): Unit = {
    featureStoreManager.loadFeatureDefinitionsFromSchema(inputDataFrame)
  }

  @Test
  def ingestDataStreamOnlineStoreWithBatchWriteRecordTest(): Unit = {
    val inputDataFrame = Seq(
      ("identifier-1", "2021-05-06T05:12:14Z"),
      ("identifier-2", "2021-05-06T05:12:14Z")
    ).toDF("record-identifier", "event-time")

    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupArn(TEST_FEATURE_GROUP_ARN)
      .featureGroupStatus(FeatureGroupStatus.CREATED)
      .eventTimeFeatureName("event-time")
      .recordIdentifierFeatureName("record-identifier")
      .featureDefinitions(
        FeatureDefinition.builder().featureName("record-identifier").featureType(FeatureType.STRING).build(),
        FeatureDefinition.builder().featureName("event-time").featureType(FeatureType.STRING).build()
      )
      .onlineStoreConfig(OnlineStoreConfig.builder().enableOnlineStore(true).build())
      .build()

    when(mockedSageMakerClient.describeFeatureGroup(any(classOf[DescribeFeatureGroupRequest]))).thenReturn(response)

    featureStoreManager.ingestData(
      inputDataFrame,
      TEST_FEATURE_GROUP_ARN,
      List("OnlineStore"),
      useLakeFormationCredentials = false,
      useBatchWriteRecord = true
    )

    verify(mockedSageMakerFeatureStoreRuntimeClient, org.mockito.Mockito.atLeastOnce())
      .batchWriteRecord(any(classOf[BatchWriteRecordRequest]))
    verify(mockedSageMakerFeatureStoreRuntimeClient, times(0)).putRecord(any(classOf[PutRecordRequest]))

    val failedDf = featureStoreManager.getFailedStreamIngestionDataFrame
    assertEquals(failedDf.count(), 0)
  }

  @Test
  def ingestDataStreamOnlineStoreWithBatchWriteRecordPartialFailureTest(): Unit = {
    val inputDataFrame = Seq(
      ("identifier-1", "2021-05-06T05:12:14Z"),
      ("identifier-2", "2021-05-06T05:12:14Z")
    ).toDF("record-identifier", "event-time")

    val failedEntry = BatchWriteRecordEntry
      .builder()
      .featureGroupName(TEST_FEATURE_GROUP_ARN)
      .record(
        java.util.Arrays.asList(
          FeatureValue.builder().featureName("record-identifier").valueAsString("identifier-2").build(),
          FeatureValue.builder().featureName("event-time").valueAsString("2021-05-06T05:12:14Z").build()
        )
      )
      .targetStores(java.util.Arrays.asList(TargetStore.ONLINE_STORE))
      .build()

    val errorResponse = BatchWriteRecordResponse
      .builder()
      .errors(
        java.util.Arrays.asList(
          BatchWriteRecordError
            .builder()
            .entry(failedEntry)
            .errorCode("ValidationError")
            .errorMessage("Test validation error")
            .build()
        )
      )
      .unprocessedEntries(java.util.Collections.emptyList[BatchWriteRecordEntry]())
      .build()

    when(mockedSageMakerFeatureStoreRuntimeClient.batchWriteRecord(any(classOf[BatchWriteRecordRequest])))
      .thenReturn(errorResponse)

    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupArn(TEST_FEATURE_GROUP_ARN)
      .featureGroupStatus(FeatureGroupStatus.CREATED)
      .eventTimeFeatureName("event-time")
      .recordIdentifierFeatureName("record-identifier")
      .featureDefinitions(
        FeatureDefinition.builder().featureName("record-identifier").featureType(FeatureType.STRING).build(),
        FeatureDefinition.builder().featureName("event-time").featureType(FeatureType.STRING).build()
      )
      .onlineStoreConfig(OnlineStoreConfig.builder().enableOnlineStore(true).build())
      .build()

    when(mockedSageMakerClient.describeFeatureGroup(any(classOf[DescribeFeatureGroupRequest]))).thenReturn(response)

    val caught = intercept[StreamIngestionFailureException] {
      featureStoreManager.ingestData(
        inputDataFrame,
        TEST_FEATURE_GROUP_ARN,
        List("OnlineStore"),
        useLakeFormationCredentials = false,
        useBatchWriteRecord = true
      )
    }

    caught.message.contains("records failed to be ingested") shouldBe true

    val failedDf = featureStoreManager.getFailedStreamIngestionDataFrame
    // Verify exactly 1 row failed (identifier-2), not both
    assertEquals(failedDf.count(), 1L)
    // Verify the correct row (identifier-2) is in the failed DataFrame
    val failedRow = failedDf.first()
    assertEquals(failedRow.getAs[String]("record-identifier"), "identifier-2")
    // Verify the error message is captured
    assertEquals(failedRow.getAs[String]("online_ingestion_error"), "ValidationError: Test validation error")
  }

  @Test
  def ingestDataStreamOnlineStoreWithBatchWriteRecordPartialFailureMultiRowTest(): Unit = {
    // 5 records: only record at index 2 (identifier-3) fails
    val inputDataFrame = Seq(
      ("identifier-1", "2021-05-06T05:12:14Z"),
      ("identifier-2", "2021-05-06T05:12:14Z"),
      ("identifier-3", "2021-05-06T05:12:14Z"),
      ("identifier-4", "2021-05-06T05:12:14Z"),
      ("identifier-5", "2021-05-06T05:12:14Z")
    ).toDF("record-identifier", "event-time")

    val failedEntry = BatchWriteRecordEntry
      .builder()
      .featureGroupName(TEST_FEATURE_GROUP_ARN)
      .record(
        java.util.Arrays.asList(
          FeatureValue.builder().featureName("record-identifier").valueAsString("identifier-3").build(),
          FeatureValue.builder().featureName("event-time").valueAsString("2021-05-06T05:12:14Z").build()
        )
      )
      .targetStores(java.util.Arrays.asList(TargetStore.ONLINE_STORE))
      .build()

    val errorResponse = BatchWriteRecordResponse
      .builder()
      .errors(
        java.util.Arrays.asList(
          BatchWriteRecordError
            .builder()
            .entry(failedEntry)
            .errorCode("InternalError")
            .errorMessage("Internal service error")
            .build()
        )
      )
      .unprocessedEntries(java.util.Collections.emptyList[BatchWriteRecordEntry]())
      .build()

    when(mockedSageMakerFeatureStoreRuntimeClient.batchWriteRecord(any(classOf[BatchWriteRecordRequest])))
      .thenReturn(errorResponse)

    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupArn(TEST_FEATURE_GROUP_ARN)
      .featureGroupStatus(FeatureGroupStatus.CREATED)
      .eventTimeFeatureName("event-time")
      .recordIdentifierFeatureName("record-identifier")
      .featureDefinitions(
        FeatureDefinition.builder().featureName("record-identifier").featureType(FeatureType.STRING).build(),
        FeatureDefinition.builder().featureName("event-time").featureType(FeatureType.STRING).build()
      )
      .onlineStoreConfig(OnlineStoreConfig.builder().enableOnlineStore(true).build())
      .build()

    when(mockedSageMakerClient.describeFeatureGroup(any(classOf[DescribeFeatureGroupRequest]))).thenReturn(response)

    val caught = intercept[StreamIngestionFailureException] {
      featureStoreManager.ingestData(
        inputDataFrame,
        TEST_FEATURE_GROUP_ARN,
        List("OnlineStore"),
        useLakeFormationCredentials = false,
        useBatchWriteRecord = true
      )
    }

    caught.message.contains("records failed to be ingested") shouldBe true

    val failedDf = featureStoreManager.getFailedStreamIngestionDataFrame
    // Only 1 out of 5 should fail
    assertEquals(failedDf.count(), 1L)
    // Verify it's specifically identifier-3 (index 2 in the batch)
    val failedRow = failedDf.first()
    assertEquals(failedRow.getAs[String]("record-identifier"), "identifier-3")
    assertEquals(failedRow.getAs[String]("online_ingestion_error"), "InternalError: Internal service error")
  }

  @Test
  def ingestDataStreamOnlineStoreWithBatchWriteRecordMultipleFailuresTest(): Unit = {
    // 5 records: indices 1 (identifier-2) and 3 (identifier-4) fail
    val inputDataFrame = Seq(
      ("identifier-1", "2021-05-06T05:12:14Z"),
      ("identifier-2", "2021-05-06T05:12:14Z"),
      ("identifier-3", "2021-05-06T05:12:14Z"),
      ("identifier-4", "2021-05-06T05:12:14Z"),
      ("identifier-5", "2021-05-06T05:12:14Z")
    ).toDF("record-identifier", "event-time")

    val failedEntry1 = BatchWriteRecordEntry
      .builder()
      .featureGroupName(TEST_FEATURE_GROUP_ARN)
      .record(
        java.util.Arrays.asList(
          FeatureValue.builder().featureName("record-identifier").valueAsString("identifier-2").build(),
          FeatureValue.builder().featureName("event-time").valueAsString("2021-05-06T05:12:14Z").build()
        )
      )
      .targetStores(java.util.Arrays.asList(TargetStore.ONLINE_STORE))
      .build()

    val failedEntry2 = BatchWriteRecordEntry
      .builder()
      .featureGroupName(TEST_FEATURE_GROUP_ARN)
      .record(
        java.util.Arrays.asList(
          FeatureValue.builder().featureName("record-identifier").valueAsString("identifier-4").build(),
          FeatureValue.builder().featureName("event-time").valueAsString("2021-05-06T05:12:14Z").build()
        )
      )
      .targetStores(java.util.Arrays.asList(TargetStore.ONLINE_STORE))
      .build()

    val errorResponse = BatchWriteRecordResponse
      .builder()
      .errors(
        java.util.Arrays.asList(
          BatchWriteRecordError
            .builder()
            .entry(failedEntry1)
            .errorCode("ValidationError")
            .errorMessage("Error on record 2")
            .build(),
          BatchWriteRecordError
            .builder()
            .entry(failedEntry2)
            .errorCode("InternalError")
            .errorMessage("Error on record 4")
            .build()
        )
      )
      .unprocessedEntries(java.util.Collections.emptyList[BatchWriteRecordEntry]())
      .build()

    when(mockedSageMakerFeatureStoreRuntimeClient.batchWriteRecord(any(classOf[BatchWriteRecordRequest])))
      .thenReturn(errorResponse)

    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupArn(TEST_FEATURE_GROUP_ARN)
      .featureGroupStatus(FeatureGroupStatus.CREATED)
      .eventTimeFeatureName("event-time")
      .recordIdentifierFeatureName("record-identifier")
      .featureDefinitions(
        FeatureDefinition.builder().featureName("record-identifier").featureType(FeatureType.STRING).build(),
        FeatureDefinition.builder().featureName("event-time").featureType(FeatureType.STRING).build()
      )
      .onlineStoreConfig(OnlineStoreConfig.builder().enableOnlineStore(true).build())
      .build()

    when(mockedSageMakerClient.describeFeatureGroup(any(classOf[DescribeFeatureGroupRequest]))).thenReturn(response)

    val caught = intercept[StreamIngestionFailureException] {
      featureStoreManager.ingestData(
        inputDataFrame,
        TEST_FEATURE_GROUP_ARN,
        List("OnlineStore"),
        useLakeFormationCredentials = false,
        useBatchWriteRecord = true
      )
    }

    caught.message.contains("records failed to be ingested") shouldBe true

    val failedDf = featureStoreManager.getFailedStreamIngestionDataFrame
    // Exactly 2 out of 5 should fail
    assertEquals(failedDf.count(), 2L)
    // Verify the correct rows failed (identifier-2 and identifier-4)
    val failedIds = failedDf.collect().map(_.getAs[String]("record-identifier")).sorted
    assertEquals(failedIds.toList, List("identifier-2", "identifier-4"))
    // Verify error messages are correct
    val failedErrors = failedDf
      .collect()
      .map(row => (row.getAs[String]("record-identifier"), row.getAs[String]("online_ingestion_error")))
      .toMap
    assertEquals(failedErrors("identifier-2"), "ValidationError: Error on record 2")
    assertEquals(failedErrors("identifier-4"), "InternalError: Error on record 4")
  }

  @Test
  def ingestDataStreamOnlineStoreWithBatchWriteRecordFullFailureTest(): Unit = {
    val inputDataFrame = Seq(
      ("identifier-1", "2021-05-06T05:12:14Z")
    ).toDF("record-identifier", "event-time")

    when(mockedSageMakerFeatureStoreRuntimeClient.batchWriteRecord(any(classOf[BatchWriteRecordRequest])))
      .thenThrow(new RuntimeException("batch write error"))

    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupArn(TEST_FEATURE_GROUP_ARN)
      .featureGroupStatus(FeatureGroupStatus.CREATED)
      .eventTimeFeatureName("event-time")
      .recordIdentifierFeatureName("record-identifier")
      .featureDefinitions(
        FeatureDefinition.builder().featureName("record-identifier").featureType(FeatureType.STRING).build(),
        FeatureDefinition.builder().featureName("event-time").featureType(FeatureType.STRING).build()
      )
      .onlineStoreConfig(OnlineStoreConfig.builder().enableOnlineStore(true).build())
      .build()

    when(mockedSageMakerClient.describeFeatureGroup(any(classOf[DescribeFeatureGroupRequest]))).thenReturn(response)

    val caught = intercept[StreamIngestionFailureException] {
      featureStoreManager.ingestData(
        inputDataFrame,
        TEST_FEATURE_GROUP_ARN,
        List("OnlineStore"),
        useLakeFormationCredentials = false,
        useBatchWriteRecord = true
      )
    }

    val failedDf = featureStoreManager.getFailedStreamIngestionDataFrame
    assertEquals(failedDf.count(), 1)
    assertEquals(failedDf.first().getAs[String]("online_ingestion_error"), "batch write error")
  }

  @Test
  def listRecordsTest(): Unit = {
    val result = featureStoreManager.listRecords(TEST_FEATURE_GROUP_ARN, maxResults = 10)

    verify(mockedSageMakerFeatureStoreRuntimeClient, org.mockito.Mockito.atLeastOnce())
      .listRecords(any(classOf[ListRecordsRequest]))
    val identifiers = result.get("RecordIdentifiers").asInstanceOf[java.util.List[String]]
    assertEquals(identifiers.size(), 2)
    assertEquals(identifiers.get(0), "id-1")
    assertEquals(identifiers.get(1), "id-2")
  }

  @Test
  def listRecordsWithNextTokenTest(): Unit = {
    val responseWithToken = ListRecordsResponse
      .builder()
      .recordIdentifiers(java.util.Arrays.asList("id-1"))
      .nextToken("next-page-token")
      .build()

    when(mockedSageMakerFeatureStoreRuntimeClient.listRecords(any(classOf[ListRecordsRequest])))
      .thenReturn(responseWithToken)

    val result = featureStoreManager.listRecords(TEST_FEATURE_GROUP_ARN, maxResults = 1)

    val nextToken = result.get("NextToken").asInstanceOf[String]
    assertEquals(nextToken, "next-page-token")
  }

  @Test
  def listRecordsWithSoftDeletedTest(): Unit = {
    val result =
      featureStoreManager.listRecords(TEST_FEATURE_GROUP_ARN, includeSoftDeletedRecords = true)

    verify(mockedSageMakerFeatureStoreRuntimeClient, org.mockito.Mockito.atLeastOnce())
      .listRecords(any(classOf[ListRecordsRequest]))
    val identifiers = result.get("RecordIdentifiers").asInstanceOf[java.util.List[String]]
    assertEquals(identifiers.size(), 2)
  }

  @DataProvider
  def ingestDataStreamOnlineStoreTestDataProvider(): Array[Array[Any]] = {
    Array(
      Array(
        Seq(("identifier-1", "2021-05-06T05:12:14Z"))
          .toDF("record-identifier", "event-time"),
        PutRecordRequest
          .builder()
          .featureGroupName(TEST_FEATURE_GROUP_ARN)
          .targetStores(List(TargetStore.ONLINE_STORE).asJava)
          .record(
            FeatureValue
              .builder()
              .featureName("record-identifier")
              .valueAsString("identifier-1")
              .build(),
            FeatureValue
              .builder()
              .featureName("event-time")
              .valueAsString("2021-05-06T05:12:14Z")
              .build()
          )
          .build(),
        List("OnlineStore")
      )
    )
  }

  @DataProvider
  def ingestDataBatchOfflineStoreGlueTableTestDataProvider(): Array[Array[Any]] = {
    val inputTestDf = Seq(("identifier-1", "2021-05-06T05:12:14Z"))
      .toDF("record-identifier", "event-time")
    val expectedPutRecordRequest = PutRecordRequest
      .builder()
      .featureGroupName("test-feature-group")
      .targetStores(List(TargetStore.OFFLINE_STORE).asJava)
      .record(
        FeatureValue
          .builder()
          .featureName("record-identifier")
          .valueAsString("identifier-1")
          .build(),
        FeatureValue
          .builder()
          .featureName("event-time")
          .valueAsString("2021-05-06T05:12:14Z")
          .build()
      )
      .build()

    Array(
      Array(
        inputTestDf,
        expectedPutRecordRequest,
        List("OfflineStore"),
        TableFormat.GLUE
      ),
      Array(
        inputTestDf,
        expectedPutRecordRequest,
        List("OfflineStore"),
        null
      )
    )
  }

  @DataProvider
  def loadFeatureDefinitionsFromSchemaTestDataProvider(): Array[Array[Any]] = {
    Array(
      Array(
        Seq((123, 100.0, "dummy"))
          .toDF("feature-integral", "feature-fractional", "feature-string"),
        List(
          FeatureDefinition.builder().featureName("feature-integral").featureType(FeatureType.INTEGRAL).build(),
          FeatureDefinition.builder().featureName("feature-fractional").featureType(FeatureType.FRACTIONAL).build(),
          FeatureDefinition.builder().featureName("feature-string").featureType(FeatureType.STRING).build()
        )
      )
    )
  }

  @DataProvider
  def loadFeatureDefinitionsFromSchemaNegativeTestDataProvider(): Array[Array[Any]] = {
    Array(
      Array(
        Seq((123, 100.0, true))
          .toDF("feature-integral", "feature-fractional", "feature-boolean")
      )
    )
  }

  def verifyDataIngestedInOfflineStore(
      inputDataFrame: DataFrame,
      resolvedOutputPath: String
  ): Unit = {
    val inputSchema = inputDataFrame.schema.names.toList
    val outputDataFrame =
      sparkSession.read.format("parquet").load(resolvedOutputPath)

    val transformedDataFrame =
      inputDataFrame.select(inputSchema.head, inputSchema: _*)
    val outputDataFrameWithOriginalColumns =
      outputDataFrame.select(inputSchema.head, inputSchema: _*)

    assertEquals(
      outputDataFrameWithOriginalColumns.first().toString(),
      transformedDataFrame.first().toString()
    )

    var validationDataFrame =
      outputDataFrame.where(col("write_time").cast("timestamp").isNull)
    assertEquals(validationDataFrame.count(), 0)

    validationDataFrame = outputDataFrame.where(col("api_invocation_time").cast("timestamp").isNull)
    assertEquals(validationDataFrame.count(), 0)

    validationDataFrame = outputDataFrame.where(col("is_deleted").cast("boolean").isNull)
    assertEquals(validationDataFrame.count(), 0)
  }

  @AfterTest
  def cleanupTestArtifact(): Unit = {
    val testArtifactDirectory = new Directory(new File(TEST_ARTIFACT_ROOT))
    testArtifactDirectory.deleteRecursively()
  }
}
