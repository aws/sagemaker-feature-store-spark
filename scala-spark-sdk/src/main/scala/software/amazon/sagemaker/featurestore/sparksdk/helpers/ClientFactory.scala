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
import software.amazon.awssdk.services.sagemaker.SageMakerClient
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.{
  SageMakerFeatureStoreRuntimeClient,
  SageMakerFeatureStoreRuntimeClientBuilder
}

/** This factory provides the default client and configurations.
 */
object ClientFactory {

  private final val DEFAULT_MAX_NUMBER_RETRIES  = 10
  private var _sageMakerClient: SageMakerClient = getDefaultSageMakerClient
  private var _sageMakerFeatureStoreRuntimeClientBuilder: SageMakerFeatureStoreRuntimeClientBuilder =
    getDefaultFeatureStoreRuntimeClientBuilder

  // Getters
  def sageMakerClient: SageMakerClient = _sageMakerClient
  def sageMakerFeatureStoreRuntimeClientBuilder: SageMakerFeatureStoreRuntimeClientBuilder =
    _sageMakerFeatureStoreRuntimeClientBuilder

  // Setters
  @VisibleForTesting
  def sageMakerClient_=(client: SageMakerClient): Unit = _sageMakerClient = client
  @VisibleForTesting
  def sageMakerFeatureStoreRuntimeClientBuilder_=(
      runtimeClientBuilder: SageMakerFeatureStoreRuntimeClientBuilder
  ): Unit = _sageMakerFeatureStoreRuntimeClientBuilder = runtimeClientBuilder

  /** Genereate default SageMakerClient
   *
   *  @return
   *    sagemaker client
   */
  def getDefaultSageMakerClient: SageMakerClient = {
    SageMakerClient
      .builder()
      .httpClient(ApacheHttpClient.builder().build())
      .build()
  }

  /** Genereate default feature store runtime client
   *
   *  @return
   *    feature store runtime client
   */
  def getDefaultFeatureStoreRuntimeClientBuilder: SageMakerFeatureStoreRuntimeClientBuilder = {
    SageMakerFeatureStoreRuntimeClient
      .builder()
      .httpClient(ApacheHttpClient.builder().build())
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .retryPolicy(getDefaultFeatureStoreRuntimeRetryPolicy)
          .build()
      )
  }

  /** Genereate default feature store runtime retry policy
   *
   *  @return
   *    feature store runtime retry policy
   */
  def getDefaultFeatureStoreRuntimeRetryPolicy: RetryPolicy = {
    RetryPolicy.builder().numRetries(DEFAULT_MAX_NUMBER_RETRIES).build()
  }

}
