package software.amazon.sagemaker.featurestore.sparksdk.helpers

import org.scalatestplus.testng.TestNGSuite
import org.testng.Assert.assertEquals
import org.testng.annotations.Test
import software.amazon.sagemaker.featurestore.sparksdk.exceptions.ValidationError

class FeatureGroupArnResolverTest extends TestNGSuite {

  private final val TEST_FEATURE_GROUP_ARN =
    "arn:aws:sagemaker:us-west-2:123456789012:feature-group/test-feature-group"

  @Test(expectedExceptions = Array(classOf[ValidationError]))
  def testFeatureGroupArnResolverWithInvalidFeatureGroupArn(): Unit = {
    new FeatureGroupArnResolver("invalid-feature-group-arm")
  }

  @Test
  def testFeatureGroupArnResolver(): Unit = {
    val resolver = new FeatureGroupArnResolver(TEST_FEATURE_GROUP_ARN)
    assertEquals(resolver.resolveFeatureGroupName(), "test-feature-group")
    assertEquals(resolver.resolveRegion(), "us-west-2")
  }
}
