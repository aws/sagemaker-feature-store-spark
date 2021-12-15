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

import software.amazon.sagemaker.featurestore.sparksdk.exceptions.ValidationError

import scala.util.matching.Regex

/** A helper to resolve information from feature group arn.
 *
 *  @param featureGroupArn
 *    Arn of a feature group
 */
class FeatureGroupArnResolver(val featureGroupArn: String) {

  private final val FEATURE_GROUP_ARN_PATTERN =
    "arn:aws[a-z\\-]*:sagemaker:[a-z0-9\\-]*:[0-9]{12}:feature-group/.*"

  verifyFeatureGroupArn()

  private final val featureGroupArnParts = featureGroupArn.split(':')

  /** Resolve featur group name from arn.
   *
   *  @return
   *    feature group name
   */
  def resolveFeatureGroupName(): String = {
    featureGroupArn.split('/')(1)
  }

  /** Resolve region from arn
   *
   *  @return
   *    region
   */
  def resolveRegion(): String = {
    featureGroupArnParts(3)
  }

  private def verifyFeatureGroupArn(): Unit = {
    val pattern = new Regex(FEATURE_GROUP_ARN_PATTERN).pattern
    if (!pattern.matcher(featureGroupArn).matches()) {
      throw ValidationError("Provided feature group arn is not valid.")
    }
  }
}
