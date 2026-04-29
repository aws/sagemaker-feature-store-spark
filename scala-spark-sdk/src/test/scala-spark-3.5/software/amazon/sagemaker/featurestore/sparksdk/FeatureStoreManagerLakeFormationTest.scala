package software.amazon.sagemaker.featurestore.sparksdk

import org.apache.spark.sql.SparkSession
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.doNothing
import org.mockito.MockitoSugar.{times, verify, when, withObjectMocked}
import org.scalatestplus.mockito.MockitoSugar.mock
import org.scalatestplus.testng.TestNGSuite
import org.testng.annotations.{BeforeMethod, Test}
import software.amazon.awssdk.services.sagemaker.SageMakerClient
import software.amazon.awssdk.services.sagemaker.model.{
  DataCatalogConfig,
  DescribeFeatureGroupRequest,
  DescribeFeatureGroupResponse,
  FeatureDefinition,
  FeatureGroupStatus,
  FeatureType,
  OfflineStoreConfig,
  S3StorageConfig,
  TableFormat
}
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.PutRecordRequest
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.{
  SageMakerFeatureStoreRuntimeClient,
  SageMakerFeatureStoreRuntimeClientBuilder
}
import software.amazon.sagemaker.featurestore.sparksdk.helpers.{ClientFactory, LakeFormationHelper}

/** Lake Formation specific tests for [[FeatureStoreManager]]. Only compiled for Spark 3.5+ where LF credential vending
 *  is supported end-to-end (the build-time gate is a no-op on 3.5+ and would throw on older builds).
 */
class FeatureStoreManagerLakeFormationTest extends TestNGSuite {

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

  @BeforeMethod
  def setup(): Unit = {
    ClientFactory.skipInitialization = true
    ClientFactory.sageMakerClient = mockedSageMakerClient
    ClientFactory.sageMakerFeatureStoreRuntimeClientBuilder = mockedSageMakerFeatureStoreRuntimeClientBuilder
    when(mockedSageMakerFeatureStoreRuntimeClientBuilder.build()).thenReturn(mockedSageMakerFeatureStoreRuntimeClient)
  }

  @Test
  def ingestDataBatchOfflineStoreSkipLakeFormationTest(): Unit = {
    val resolvedOutputPath =
      TEST_ARTIFACT_ROOT + "/ingest-data-direct-offline-store-test/skip-lf"
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
          .tableFormat(TableFormat.GLUE)
          .dataCatalogConfig(
            DataCatalogConfig
              .builder()
              .catalog("glue")
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

    val inputDataFrame = Seq(("identifier-1", "2021-05-06T05:12:14Z"))
      .toDF("record-identifier", "event-time")

    withObjectMocked[ClientFactory.type] {
      when(ClientFactory.sageMakerClient).thenReturn(mockedSageMakerClient)
      doNothing().when(ClientFactory).initialize(anyString(), anyString())

      withObjectMocked[LakeFormationHelper.type] {
        featureStoreManager.ingestData(
          inputDataFrame,
          TEST_FEATURE_GROUP_ARN,
          List("OfflineStore"),
          useLakeFormationCredentials = false
        )

        verify(LakeFormationHelper, times(0))
          .vendCredentials(anyString(), anyString(), anyString(), anyString(), anyString())
      }
    }
  }
}
