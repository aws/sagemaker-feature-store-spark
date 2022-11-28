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

package software.amazon.sagemaker.featurestore.sparksdk.helpers

import com.google.common.annotations.VisibleForTesting
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sagemaker.SageMakerClient
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.{
  SageMakerFeatureStoreRuntimeClient,
  SageMakerFeatureStoreRuntimeClientBuilder
}
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import java.net.URI
import java.util.UUID

/** This factory provides the default client and configurations.
 */
object ClientFactory {

  private final val DEFAULT_MAX_NUMBER_RETRIES                                                              = 10
  private var _assumeRoleArn: Option[String]                                                                = None
  private var _region: Option[String]                                                                       = None
  private var _stsAssumeRoleCredentialsProvider: Option[StsAssumeRoleCredentialsProvider]                   = None
  private var _sageMakerClient: Option[SageMakerClient]                                                     = None
  private var _sageMakerFeatureStoreRuntimeClientBuilder: Option[SageMakerFeatureStoreRuntimeClientBuilder] = None
  private var _skipInitialization: Boolean                                                                  = false

  // Getters
  def sageMakerClient: SageMakerClient = _sageMakerClient.orNull
  def sageMakerFeatureStoreRuntimeClientBuilder: SageMakerFeatureStoreRuntimeClientBuilder =
    _sageMakerFeatureStoreRuntimeClientBuilder.orNull
  def assumeRoleArn: String                                              = _assumeRoleArn.orNull
  def region: String                                                     = _region.orNull
  def stsAssumeRoleCredentialsProvider: StsAssumeRoleCredentialsProvider = _stsAssumeRoleCredentialsProvider.orNull
  def skipInitialization: Boolean                                        = _skipInitialization

  // Setters
  @VisibleForTesting
  def sageMakerClient_=(client: SageMakerClient): Unit = _sageMakerClient = Option(client)
  @VisibleForTesting
  def sageMakerFeatureStoreRuntimeClientBuilder_=(
      runtimeClientBuilder: SageMakerFeatureStoreRuntimeClientBuilder
  ): Unit = _sageMakerFeatureStoreRuntimeClientBuilder = Option(runtimeClientBuilder)
  @VisibleForTesting
  def assumeRoleArn_=(roleArn: String): Unit = _assumeRoleArn = Option(roleArn)
  @VisibleForTesting
  def region_=(region: String): Unit = _region = Option(region)
  @VisibleForTesting
  def stsAssumeRoleCredentialsProvider_=(credentialsProvider: StsAssumeRoleCredentialsProvider): Unit =
    _stsAssumeRoleCredentialsProvider = Option(credentialsProvider)
  @VisibleForTesting
  def skipInitialization_=(skipInitialization: Boolean): Unit = _skipInitialization = skipInitialization

  /** Initialize the client factory
   *
   *  @param roleArn
   *    Initialize the client factory with provided role arn
   *  @param region
   *    Initialize the client factory with provided region, region should be the same as what provided in feature group
   *    arn
   */
  def initialize(region: String, roleArn: String = null): Unit = {
    if (skipInitialization) {
      return
    }

    this.assumeRoleArn = roleArn
    this.region = region
    this.stsAssumeRoleCredentialsProvider = getStsAssumeRoleCredentialsProvider
    this.sageMakerClient = getDefaultSageMakerClient
    this.sageMakerFeatureStoreRuntimeClientBuilder = getDefaultFeatureStoreRuntimeClientBuilder
  }

  private def getDefaultSageMakerClient: SageMakerClient = {
    val sageMakerClientBuilder = SageMakerClient
      .builder()
      .region(Region.of(region))
      .httpClient(ApacheHttpClient.builder().build())

    if (_assumeRoleArn.nonEmpty) {
      sageMakerClientBuilder.credentialsProvider(stsAssumeRoleCredentialsProvider)
    }

    sageMakerClientBuilder.build()
  }

  def getDefaultFeatureStoreRuntimeClientBuilder: SageMakerFeatureStoreRuntimeClientBuilder = {
    val sageMakerFeatureStoreRuntimeClient =
      SageMakerFeatureStoreRuntimeClient
        .builder()
        .region(Region.of(region))
        .httpClient(ApacheHttpClient.builder().build())
        .overrideConfiguration(
          ClientOverrideConfiguration
            .builder()
            .retryPolicy(getDefaultFeatureStoreRuntimeRetryPolicy)
            .build()
        )

    if (_assumeRoleArn.nonEmpty) {
      sageMakerFeatureStoreRuntimeClient.credentialsProvider(stsAssumeRoleCredentialsProvider)
    }

    sageMakerFeatureStoreRuntimeClient
  }

  private def getDefaultFeatureStoreRuntimeRetryPolicy: RetryPolicy = {
    RetryPolicy.builder().numRetries(DEFAULT_MAX_NUMBER_RETRIES).build()
  }

  private def getStsAssumeRoleCredentialsProvider: StsAssumeRoleCredentialsProvider = {
    if (_assumeRoleArn.isEmpty) {
      return null
    }

    val assumeRoleRequest = AssumeRoleRequest
      .builder()
      .roleSessionName("feature-store-spark-" + UUID.randomUUID())
      .roleArn(assumeRoleArn)
      .build()
    val stsClient = StsClient.builder().httpClient(ApacheHttpClient.builder().build()).region(Region.of(region)).build()

    StsAssumeRoleCredentialsProvider.builder.stsClient(stsClient).refreshRequest(assumeRoleRequest).build()
  }
}
