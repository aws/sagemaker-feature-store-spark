package software.amazon.sagemaker.featurestore.sparksdk.helpers

import org.scalatestplus.testng.TestNGSuite
import org.testng.Assert.assertEquals
import org.testng.annotations.Test

/** Lake Formation specific tests for FeatureGroupArnResolver. Only compiled for Spark 3.5+ because LF support is gated
 *  to that version.
 */
class FeatureGroupArnResolverLakeFormationTest extends TestNGSuite {

  private final val TEST_FEATURE_GROUP_ARN =
    "arn:aws:sagemaker:us-west-2:123456789012:feature-group/test-feature-group"

  @Test
  def testResolveAccountId(): Unit = {
    val resolver = new FeatureGroupArnResolver(TEST_FEATURE_GROUP_ARN)
    assertEquals(resolver.resolveAccountId(), "123456789012")
  }

  @Test
  def testResolvePartition(): Unit = {
    val resolver = new FeatureGroupArnResolver(TEST_FEATURE_GROUP_ARN)
    assertEquals(resolver.resolvePartition(), "aws")
  }

  @Test
  def testResolvePartitionChina(): Unit = {
    val resolver =
      new FeatureGroupArnResolver("arn:aws-cn:sagemaker:cn-north-1:123456789012:feature-group/test-fg")
    assertEquals(resolver.resolvePartition(), "aws-cn")
    assertEquals(resolver.resolveAccountId(), "123456789012")
    assertEquals(resolver.resolveRegion(), "cn-north-1")
  }

  @Test
  def testResolvePartitionGovCloud(): Unit = {
    val resolver =
      new FeatureGroupArnResolver("arn:aws-us-gov:sagemaker:us-gov-west-1:123456789012:feature-group/test-fg")
    assertEquals(resolver.resolvePartition(), "aws-us-gov")
    assertEquals(resolver.resolveRegion(), "us-gov-west-1")
  }
}
