package software.amazon.sagemaker.featurestore.sparksdk

import collection.JavaConverters._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{times, verify, when}
import org.mockito.captor.ArgCaptor
import org.scalatest.PrivateMethodTester
import org.scalatestplus.mockito.MockitoSugar.mock
import org.scalatestplus.testng.TestNGSuite
import org.testng.Assert.assertEquals
import org.testng.annotations.{AfterTest, BeforeClass, DataProvider, Test}
import software.amazon.awssdk.services.sagemaker.SageMakerClient
import software.amazon.awssdk.services.sagemaker.model.{
  DescribeFeatureGroupRequest,
  DescribeFeatureGroupResponse,
  FeatureDefinition,
  FeatureGroupStatus,
  FeatureType,
  OfflineStoreConfig,
  OnlineStoreConfig,
  S3StorageConfig
}
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.{
  FeatureValue,
  PutRecordRequest,
  PutRecordResponse
}
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.{
  SageMakerFeatureStoreRuntimeClient,
  SageMakerFeatureStoreRuntimeClientBuilder
}
import software.amazon.sagemaker.featurestore.sparksdk.exceptions.ValidationError
import software.amazon.sagemaker.featurestore.sparksdk.helpers.ClientFactory

import java.io.File
import scala.reflect.io.Directory
import scala.Double.NaN

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

  @Test(dataProvider = "ingestDataTestDataProvider")
  def ingestDataStreamOnlineStoreTest(inputDataFrame: DataFrame, expectedPutRecordRequest: PutRecordRequest): Unit = {
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
      TEST_FEATURE_GROUP_ARN
    )
    verify(mockedSageMakerFeatureStoreRuntimeClient).putRecord(
      putRecordRequestCaptor
    )
    putRecordRequestCaptor.hasCaptured(expectedPutRecordRequest)
  }

  @Test(dataProvider = "ingestDataTestDataProvider")
  def ingestDataDirectOfflineStoreTest_onlyOfflineStoreEnabled(
      inputDataFrame: DataFrame,
      putRecordRequest: PutRecordRequest
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
    featureStoreManager.ingestData(
      inputDataFrame,
      TEST_FEATURE_GROUP_ARN
    )
    verify(mockedSageMakerFeatureStoreRuntimeClient, times(0))
      .putRecord(putRecordRequest)
    verifyDataIngestedInOfflineStore(inputDataFrame, resolvedOutputPath)
  }

  @Test(dataProvider = "ingestDataTestDataProvider")
  def ingestDataDirectOfflineStoreTest_directOfflineStoreSpecified(
      inputDataFrame: DataFrame,
      putRecordRequest: PutRecordRequest
  ): Unit = {
    val resolvedOutputPath =
      TEST_ARTIFACT_ROOT + "/ingest-data-direct-offline-store-test/direct-offline-store-specified"
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
        OnlineStoreConfig.builder().enableOnlineStore(true).build()
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
    featureStoreManager.ingestData(
      inputDataFrame,
      TEST_FEATURE_GROUP_ARN,
      directOfflineStore = true
    )
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

  @Test(
    expectedExceptions = Array(classOf[ValidationError]),
    dataProvider = "validateSchemaNegativeTestDataProvider"
  )
  def validateDataFrameSchemaTest_negative(
      inputDataFrame: DataFrame,
      feature_group_arn: String,
      feature_group_status: FeatureGroupStatus
  ) {
    val response = buildTestDescribeFeatureGroupResponse(feature_group_arn, feature_group_status)

    when(mockedSageMakerClient.describeFeatureGroup(any(classOf[DescribeFeatureGroupRequest]))).thenReturn(response)

    featureStoreManager.validateDataFrameSchema(
      inputDataFrame,
      TEST_FEATURE_GROUP_ARN
    )
  }

  @Test(
    dataProvider = "validateSchemaPositiveTestDataProvider"
  )
  def validateDataFrameSchemaTest_positive(inputDataFrame: DataFrame) {
    val response = buildTestDescribeFeatureGroupResponse(TEST_FEATURE_GROUP_ARN, FeatureGroupStatus.CREATED)

    when(mockedSageMakerClient.describeFeatureGroup(any(classOf[DescribeFeatureGroupRequest]))).thenReturn(response)

    featureStoreManager.validateDataFrameSchema(
      inputDataFrame,
      TEST_FEATURE_GROUP_ARN
    )
  }

  @DataProvider
  def ingestDataTestDataProvider(): Array[Array[Any]] = {
    Array(
      Array(
        Seq(("identifier-1", "2021-05-06T05:12:14Z"))
          .toDF("record-identifier", "event-time"),
        PutRecordRequest
          .builder()
          .featureGroupName("test-feature-group")
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

  @DataProvider
  def validateSchemaNegativeTestDataProvider(): Array[Array[Any]] = {
    Array(
      // Column names contain invalid char
      Array(Seq(("identifier")).toDF("{record-identifier}"), TEST_FEATURE_GROUP_ARN, FeatureGroupStatus.CREATED),
      // Columns contain unknow feature
      Array(Seq(("feature")).toDF("feature-unknown"), TEST_FEATURE_GROUP_ARN, FeatureGroupStatus.CREATED),
      // Missing required feature name in the data frame
      Array(Seq(("identifier-1")).toDF("record-identifier"), TEST_FEATURE_GROUP_ARN, FeatureGroupStatus.CREATED),
      Array(Seq(("1631091971")).toDF("event-time"), TEST_FEATURE_GROUP_ARN, FeatureGroupStatus.CREATED),
      // Columns contain reserved feature names
      Array(
        Seq(("identifier-1", "1631091971", "true"))
          .toDF("record-identifier", "event-time", "is_deleted"),
        TEST_FEATURE_GROUP_ARN,
        FeatureGroupStatus.CREATED
      ),
      // Invalid feature values
      Array(
        Seq(("identifier-1", "invalid-event-time"))
          .toDF("record-identifier", "event-time"),
        TEST_FEATURE_GROUP_ARN,
        FeatureGroupStatus.CREATED
      ),
      Array(
        Seq(("identifier-1", "1631091971", "invalid-integral"))
          .toDF("record-identifier", "event-time", "feature-integral"),
        TEST_FEATURE_GROUP_ARN,
        FeatureGroupStatus.CREATED
      ),
      Array(
        Seq(("identifier-1", "1631091971", "invalid-fractional"))
          .toDF("record-identifier", "event-time", "feature-fractional"),
        TEST_FEATURE_GROUP_ARN,
        FeatureGroupStatus.CREATED
      ),
      Array(
        Seq(("identifier-1", NaN))
          .toDF("record-identifier", "event-time"),
        TEST_FEATURE_GROUP_ARN,
        FeatureGroupStatus.CREATED
      ),
      Array(
        Seq((null, "1631091971"))
          .toDF("record-identifier", "event-time"),
        TEST_FEATURE_GROUP_ARN,
        FeatureGroupStatus.CREATED
      ),
      Array(
        Seq(("identifier-1", null))
          .toDF("record-identifier", "event-time"),
        TEST_FEATURE_GROUP_ARN,
        FeatureGroupStatus.CREATED
      ),
      // Feature group ARN not matching
      Array(
        Seq(("identifier-1", "1631091971", "0.005", "test-feature", "100"))
          .toDF("record-identifier", "event-time", "feature-fractional", "feature-string", "feature-integral"),
        "random-feature-group-arn",
        FeatureGroupStatus.CREATED
      ),
      // Feature group is not successfully created
      Array(
        Seq(("identifier-1", "1631091971", "0.005", "test-feature", "100"))
          .toDF("record-identifier", "event-time", "feature-fractional", "feature-string", "feature-integral"),
        TEST_FEATURE_GROUP_ARN,
        FeatureGroupStatus.CREATING
      )
    )
  }

  @DataProvider
  def validateSchemaPositiveTestDataProvider(): Array[Array[Any]] = {
    Array(
      Array(
        Seq(("identifier-1", "1631091971", "0.005", "test-feature", "100"))
          .toDF("record-identifier", "event-time", "feature-fractional", "feature-string", "feature-integral")
      )
    )
  }

  def buildTestDescribeFeatureGroupResponse(
      feature_group_arn: String,
      feature_group_status: FeatureGroupStatus
  ): DescribeFeatureGroupResponse = {
    DescribeFeatureGroupResponse
      .builder()
      .featureGroupArn(feature_group_arn)
      .featureGroupStatus(feature_group_status)
      .recordIdentifierFeatureName("record-identifier")
      .eventTimeFeatureName("event-time")
      .featureDefinitions(
        FeatureDefinition
          .builder()
          .featureName("record-identifier")
          .featureType(FeatureType.STRING)
          .build(),
        FeatureDefinition
          .builder()
          .featureName("event-time")
          .featureType(FeatureType.FRACTIONAL)
          .build(),
        FeatureDefinition
          .builder()
          .featureName("feature-string")
          .featureType(FeatureType.STRING)
          .build(),
        FeatureDefinition
          .builder()
          .featureName("feature-integral")
          .featureType(FeatureType.INTEGRAL)
          .build(),
        FeatureDefinition
          .builder()
          .featureName("feature-fractional")
          .featureType(FeatureType.FRACTIONAL)
          .build()
      )
      .build()
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
