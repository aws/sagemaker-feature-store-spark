package com.amazonaws.services.sagemaker.featurestore.sparksdk.helpers

import com.amazonaws.services.sagemaker.featurestore.sparksdk.exceptions.ValidationError
import org.scalatestplus.testng.TestNGSuite
import org.testng.Assert.assertEquals
import org.testng.annotations.{DataProvider, Test}
import software.amazon.awssdk.services.sagemaker.model.{
  DescribeFeatureGroupResponse,
  FeatureGroupStatus,
  OfflineStoreConfig,
  OnlineStoreConfig,
  S3StorageConfig
}

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

  @Test()
  def checkIfFeatureGroupArnIdenticalTest_positive(): Unit = {
    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupArn(TEST_FEATURE_GROUP_ARN)
      .build()

    FeatureGroupHelper.checkIfFeatureGroupArnIdentical(response, TEST_FEATURE_GROUP_ARN)
  }

  @Test(expectedExceptions = Array(classOf[ValidationError]))
  def checkIfFeatureGroupArnIdenticalTest_negative(): Unit = {
    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupArn(TEST_FEATURE_GROUP_ARN)
      .build()

    FeatureGroupHelper.checkIfFeatureGroupArnIdentical(response, "invalid-feature-group-arn")
  }

  @Test
  def checkDirectOfflineStoreTest_positive(): Unit = {
    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupName(TEST_FEATURE_GROUP_NAME)
      .featureGroupStatus(FeatureGroupStatus.CREATED)
      .onlineStoreConfig(OnlineStoreConfig.builder().build())
      .offlineStoreConfig(OfflineStoreConfig.builder().build())
      .build()

    FeatureGroupHelper.checkDirectOfflineStore(response, directOfflineStore = true)
    FeatureGroupHelper.checkDirectOfflineStore(response, directOfflineStore = false)
  }

  @Test(expectedExceptions = Array(classOf[ValidationError]))
  def checkDirectOfflineStoreTest_negative(): Unit = {
    val response = DescribeFeatureGroupResponse
      .builder()
      .featureGroupName(TEST_FEATURE_GROUP_NAME)
      .featureGroupStatus(FeatureGroupStatus.CREATED)
      .build()

    FeatureGroupHelper.checkDirectOfflineStore(response, directOfflineStore = true)
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
}
