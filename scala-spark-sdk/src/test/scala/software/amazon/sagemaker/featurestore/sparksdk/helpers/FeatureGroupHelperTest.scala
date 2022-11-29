package software.amazon.sagemaker.featurestore.sparksdk.helpers

import org.scalatestplus.testng.TestNGSuite
import org.testng.Assert.assertEquals
import org.testng.annotations.{DataProvider, Test}
import software.amazon.awssdk.services.sagemaker.model.{
  DescribeFeatureGroupResponse,
  FeatureGroupStatus,
  OfflineStoreConfig,
  OnlineStoreConfig,
  S3StorageConfig,
  TableFormat
}
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.TargetStore
import software.amazon.sagemaker.featurestore.sparksdk.exceptions.ValidationError

class FeatureGroupHelperTest extends TestNGSuite {

  private final val TEST_FEATURE_GROUP_NAME = "test-feature-group-name"
  private final val TEST_RESOLVED_OUTPUT_S3_URI =
    "s3://test/resolved/output/s3/uri"
  private final val TEST_FEATURE_GROUP_ARN = "test-feature-group-arn"

  @Test
  def checkIfFeatureGroupIsCreatedTest_positive(): Unit = {
    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupName(TEST_FEATURE_GROUP_NAME)
      .featureGroupStatus(FeatureGroupStatus.CREATED)
      .build()

    FeatureGroupHelper.checkIfFeatureGroupIsCreated(response)
  }

  @Test(expectedExceptions = Array(classOf[ValidationError]))
  def checkIfFeatureGroupIsCreatedTest_negative(): Unit = {
    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupName(TEST_FEATURE_GROUP_NAME)
      .featureGroupStatus(FeatureGroupStatus.CREATING)
      .build()

    FeatureGroupHelper.checkIfFeatureGroupIsCreated(response)
  }

  @Test(dataProvider = "checkAndParseTargetStoreTestPositiveDataProvider")
  def checkAndParseTargetStoreTest_positive(
      response: DescribeFeatureGroupResponse,
      targetStores: List[String],
      expectedTargetStores: List[TargetStore]
  ): Unit = {
    assertEquals(FeatureGroupHelper.checkAndParseTargetStore(response, targetStores), expectedTargetStores)
  }

  @Test(dataProvider = "checkAndParseTargetStoreTestNegativeDataProvider")
  def checkAndParseTargetStoreTest_negative(
      response: DescribeFeatureGroupResponse,
      targetStores: List[String],
      expectedErrorMessage: String
  ): Unit = {
    val caught = intercept[ValidationError] {
      FeatureGroupHelper.checkAndParseTargetStore(response, targetStores)
    }
    assertEquals(caught.message, expectedErrorMessage)
  }

  @Test(dataProvider = "retrieveTargetStoresFromFeatureGroupTestDataProvider")
  def retrieveTargetStoresFromFeatureGroupTest(
      response: DescribeFeatureGroupResponse,
      expectedTargetStores: List[TargetStore]
  ): Unit = {
    val retrievedStores = FeatureGroupHelper.retrieveTargetStoresFromFeatureGroup(response)

    assertEquals(retrievedStores, expectedTargetStores)
  }

  @Test(dataProvider = "featureGroupOnlineStoreEnabledTestDataProvider")
  def featureGroupOnlineStoreEnabledTest(
      response: DescribeFeatureGroupResponse,
      expectedIfOnlineStoreEnabled: Boolean
  ): Unit = {
    assertEquals(
      FeatureGroupHelper.isFeatureGroupOnlineStoreEnabled(response),
      expectedIfOnlineStoreEnabled
    )
  }

  @Test(dataProvider = "featureGroupOfflineStoreEnabledTestDataProvider")
  def featureGroupOfflineStoreEnabledTest(
      response: DescribeFeatureGroupResponse,
      expectedIfOfflineStoreEnabled: Boolean
  ): Unit = {
    assertEquals(
      FeatureGroupHelper.isFeatureGroupOfflineStoreEnabled(response),
      expectedIfOfflineStoreEnabled
    )
  }

  @Test
  def generateDestinationFilePathTest(): Unit = {
    val response = DescribeFeatureGroupResponse
      .builder()
      .offlineStoreConfig(
        OfflineStoreConfig
          .builder()
          .s3StorageConfig(
            S3StorageConfig
              .builder()
              .resolvedOutputS3Uri(TEST_RESOLVED_OUTPUT_S3_URI)
              .build()
          )
          .build()
      )
      .build()

    assertEquals(
      FeatureGroupHelper.generateDestinationFilePath(response),
      "s3a://test/resolved/output/s3/uri"
    )
  }

  @Test(dataProvider = "isIcebergTableEnabledTestDataProvider")
  def isIcebergTableEnabledTest(describeResponse: DescribeFeatureGroupResponse, expectedResult: Boolean): Unit = {
    assertEquals(FeatureGroupHelper.isIcebergTableEnabled(describeResponse), expectedResult)
  }

  @Test(dataProvider = "isGlueTableEnabledTestDataProvider")
  def isGlueTableEnabledTest(describeResponse: DescribeFeatureGroupResponse, expectedResult: Boolean): Unit = {
    assertEquals(FeatureGroupHelper.isGlueTableEnabled(describeResponse), expectedResult)
  }

  @DataProvider
  def featureGroupOnlineStoreEnabledTestDataProvider(): Array[Array[Any]] = {
    Array(
      Array(DescribeFeatureGroupResponse.builder().build(), false),
      Array(
        DescribeFeatureGroupResponse
          .builder()
          .onlineStoreConfig(
            OnlineStoreConfig
              .builder()
              .enableOnlineStore(false)
              .build()
          )
          .build(),
        false
      ),
      Array(
        DescribeFeatureGroupResponse
          .builder()
          .onlineStoreConfig(
            OnlineStoreConfig
              .builder()
              .enableOnlineStore(true)
              .build()
          )
          .build(),
        true
      )
    )
  }

  @DataProvider
  def featureGroupOfflineStoreEnabledTestDataProvider(): Array[Array[Any]] = {
    Array(
      Array(DescribeFeatureGroupResponse.builder().build(), false),
      Array(
        DescribeFeatureGroupResponse
          .builder()
          .offlineStoreConfig(OfflineStoreConfig.builder().build())
          .build(),
        true
      )
    )
  }

  @DataProvider
  def checkAndParseTargetStoreTestPositiveDataProvider(): Array[Array[Any]] = {

    val response = DescribeFeatureGroupResponse
      .builder()
      .onlineStoreConfig(OnlineStoreConfig.builder().enableOnlineStore(true).build())
      .offlineStoreConfig(OfflineStoreConfig.builder().s3StorageConfig(S3StorageConfig.builder().build()).build())
      .build()

    Array(
      Array(response, List("OnlineStore"), List(TargetStore.ONLINE_STORE)),
      Array(response, List("OfflineStore"), List(TargetStore.OFFLINE_STORE)),
      Array(response, List("OnlineStore", "OfflineStore"), List(TargetStore.ONLINE_STORE, TargetStore.OFFLINE_STORE))
    )
  }

  @DataProvider
  def checkAndParseTargetStoreTestNegativeDataProvider(): Array[Array[Any]] = {

    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupName("test-fg-name")
      .build()

    Array(
      Array(
        response,
        List("OnlineStore"),
        "[OnlineStore] of FeatureGroup: 'test-fg-name' are not enabled, however they are specified in target store."
      ),
      Array(
        response,
        List("OfflineStore"),
        "[OfflineStore] of FeatureGroup: 'test-fg-name' are not enabled, however they are specified in target store."
      ),
      Array(response, List(null), "Found unknown target store, the valid values are [OnlineStore, OfflineStore]."),
      Array(
        response,
        List("invalid-store"),
        "Found unknown target store, the valid values are [OnlineStore, OfflineStore]."
      ),
      Array(
        response,
        List(),
        "Target stores cannot be empty, please provide at least one target store or leave it unspecified."
      )
    )
  }

  @DataProvider
  def retrieveTargetStoresFromFeatureGroupTestDataProvider(): Array[Array[Any]] = {

    Array(
      Array(
        DescribeFeatureGroupResponse
          .builder()
          .onlineStoreConfig(
            OnlineStoreConfig
              .builder()
              .enableOnlineStore(true)
              .build()
          )
          .build(),
        List(TargetStore.ONLINE_STORE)
      ),
      Array(
        DescribeFeatureGroupResponse
          .builder()
          .offlineStoreConfig(
            OfflineStoreConfig
              .builder()
              .s3StorageConfig(S3StorageConfig.builder().build())
              .build()
          )
          .build(),
        List(TargetStore.OFFLINE_STORE)
      ),
      Array(
        DescribeFeatureGroupResponse
          .builder()
          .onlineStoreConfig(
            OnlineStoreConfig
              .builder()
              .enableOnlineStore(true)
              .build()
          )
          .offlineStoreConfig(
            OfflineStoreConfig
              .builder()
              .s3StorageConfig(S3StorageConfig.builder().build())
              .build()
          )
          .build(),
        List(TargetStore.ONLINE_STORE, TargetStore.OFFLINE_STORE)
      )
    )
  }

  @DataProvider
  def isIcebergTableEnabledTestDataProvider(): Array[Array[Any]] = {
    Array(
      Array(
        DescribeFeatureGroupResponse
          .builder()
          .offlineStoreConfig(OfflineStoreConfig.builder().tableFormat(TableFormat.ICEBERG).build())
          .build(),
        true
      ),
      Array(
        DescribeFeatureGroupResponse
          .builder()
          .offlineStoreConfig(OfflineStoreConfig.builder().tableFormat(TableFormat.GLUE).build())
          .build(),
        false
      ),
      Array(DescribeFeatureGroupResponse.builder().build(), false)
    )
  }

  @DataProvider
  def isGlueTableEnabledTestDataProvider(): Array[Array[Any]] = {
    Array(
      Array(
        DescribeFeatureGroupResponse
          .builder()
          .offlineStoreConfig(OfflineStoreConfig.builder().tableFormat(TableFormat.ICEBERG).build())
          .build(),
        false
      ),
      Array(
        DescribeFeatureGroupResponse
          .builder()
          .offlineStoreConfig(OfflineStoreConfig.builder().tableFormat(TableFormat.GLUE).build())
          .build(),
        true
      ),
      Array(DescribeFeatureGroupResponse.builder().build(), false)
    )
  }
}
