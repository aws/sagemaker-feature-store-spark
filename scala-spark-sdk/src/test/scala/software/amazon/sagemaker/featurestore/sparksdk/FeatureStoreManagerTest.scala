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
import org.testng.annotations.{AfterTest, BeforeClass, DataProvider, Test}
import software.amazon.awssdk.services.sagemaker.SageMakerClient
import software.amazon.awssdk.services.sagemaker.model.{DescribeFeatureGroupRequest, DescribeFeatureGroupResponse, FeatureDefinition, FeatureGroupStatus, FeatureType, OfflineStoreConfig, OnlineStoreConfig, S3StorageConfig}
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.{FeatureValue, PutRecordRequest, PutRecordResponse, TargetStore}
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.{SageMakerFeatureStoreRuntimeClient, SageMakerFeatureStoreRuntimeClientBuilder}
import software.amazon.sagemaker.featurestore.sparksdk.exceptions.{StreamIngestionFailureException, ValidationError}
import software.amazon.sagemaker.featurestore.sparksdk.helpers.ClientFactory

import java.io.File
import scala.reflect.io.Directory

class FeatureStoreManagerTest extends TestNGSuite with PrivateMethodTester {

  private final val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("TestProgram")
    .master("local")
    .getOrCreate()
  import sparkSession.implicits._

  private final val TEST_FEATURE_GROUP_ARN = "arn:aws:sagemaker:us-west-2:123456789012:feature-group/test-feature-group"
  private final val TEST_ARTIFACT_ROOT     = "./test-artifact"

  private final val featureStoreManager = new FeatureStoreManager()

  private final val mockedSageMakerFeatureStoreRuntimeClientBuilder = mock[SageMakerFeatureStoreRuntimeClientBuilder]
  private final val mockedSageMakerClient                           = mock[SageMakerClient]
  private final val mockedSageMakerFeatureStoreRuntimeClient        = mock[SageMakerFeatureStoreRuntimeClient]
  private final val putRecordRequestCaptor                          = ArgCaptor[PutRecordRequest]

  @BeforeClass
  def setup(): Unit = {
    ClientFactory.sageMakerClient = mockedSageMakerClient
    ClientFactory.sageMakerFeatureStoreRuntimeClientBuilder = mockedSageMakerFeatureStoreRuntimeClientBuilder

    when(mockedSageMakerFeatureStoreRuntimeClientBuilder.build()).thenReturn(mockedSageMakerFeatureStoreRuntimeClient)
    when(mockedSageMakerFeatureStoreRuntimeClient.putRecord(any(classOf[PutRecordRequest])))
      .thenReturn(PutRecordResponse.builder().build())
  }

  @Test(dataProvider = "ingestDataStreamOnlineStoreTestDataProvider")
  def ingestDataStreamOnlineStoreTest(inputDataFrame: DataFrame, expectedPutRecordRequest: PutRecordRequest, inputTargetStores: List[String]): Unit = {
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

    withObjectMocked[ClientFactory.type] {
      when(ClientFactory.sageMakerClient).thenReturn(mockedSageMakerClient)
      when(ClientFactory.sageMakerFeatureStoreRuntimeClientBuilder).thenReturn(mockedSageMakerFeatureStoreRuntimeClientBuilder)
      when(mockedSageMakerClient.describeFeatureGroup(any(classOf[DescribeFeatureGroupRequest]))).thenReturn(response)

      doNothing().when(ClientFactory).initialize(anyString(), anyString())

      featureStoreManager.ingestData(
        inputDataFrame,
        TEST_FEATURE_GROUP_ARN,
        inputTargetStores
      )
      verify(mockedSageMakerFeatureStoreRuntimeClient).putRecord(
        putRecordRequestCaptor
      )
      putRecordRequestCaptor.hasCaptured(expectedPutRecordRequest)

      val failedOnlineIngestionDataFrame = featureStoreManager.getFailedOnlineIngestionDataFrame

      assertEquals(failedOnlineIngestionDataFrame.count(), 0)
    }
  }

  @Test(dataProvider = "ingestDataStreamOnlineStoreTestDataProvider")
  def ingestDataStreamOnlineStoreWithFailuresTest(inputDataFrame: DataFrame, expectedPutRecordRequest: PutRecordRequest, inputTargetStores: List[String]): Unit = {
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

    withObjectMocked[ClientFactory.type] {
      when(ClientFactory.sageMakerClient).thenReturn(mockedSageMakerClient)
      when(ClientFactory.sageMakerFeatureStoreRuntimeClientBuilder).thenReturn(mockedSageMakerFeatureStoreRuntimeClientBuilder)
      when(mockedSageMakerFeatureStoreRuntimeClient.putRecord(any(classOf[PutRecordRequest]))).thenThrow(new RuntimeException("test error"))
      when(mockedSageMakerClient.describeFeatureGroup(any(classOf[DescribeFeatureGroupRequest]))).thenReturn(response)

      doNothing().when(ClientFactory).initialize(anyString(), anyString())

      val caught = intercept[StreamIngestionFailureException] {
        featureStoreManager.ingestData(
          inputDataFrame,
          TEST_FEATURE_GROUP_ARN,
          inputTargetStores
        )
      }

      caught.message shouldBe "Stream ingestion finished, however 1 records failed to be ingested. Please inspect FailedOnlineIngestionDataFrame for more info."

      val failedOnlineIngestionDataFrame = featureStoreManager.getFailedOnlineIngestionDataFrame


      assertEquals(failedOnlineIngestionDataFrame.count(), inputDataFrame.count())
      assertEquals(failedOnlineIngestionDataFrame.first().getAs[String]("online_ingestion_error"), "test error")
    }
  }

  @Test(dataProvider = "ingestDataBatchOfflineStoreTestDataProvider")
  def ingestDataBatchOfflineStoreTest(
      inputDataFrame: DataFrame,
      putRecordRequest: PutRecordRequest,
      targetStores: List[String]
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

  @DataProvider
  def ingestDataStreamOnlineStoreTestDataProvider(): Array[Array[Any]] = {
    Array(
      Array(
        Seq(("identifier-1", "2021-05-06T05:12:14Z"))
          .toDF("record-identifier", "event-time"),
        PutRecordRequest
          .builder()
          .featureGroupName("test-feature-group")
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
  def ingestDataBatchOfflineStoreTestDataProvider(): Array[Array[Any]] = {
    Array(
      Array(
        Seq(("identifier-1", "2021-05-06T05:12:14Z"))
          .toDF("record-identifier", "event-time"),
        PutRecordRequest
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
          .build(),
        List("OfflineStore")
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
