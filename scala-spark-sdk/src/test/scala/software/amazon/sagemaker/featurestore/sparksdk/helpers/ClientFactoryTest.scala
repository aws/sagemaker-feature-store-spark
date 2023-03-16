package software.amazon.sagemaker.featurestore.sparksdk.helpers

import org.scalatestplus.testng.TestNGSuite
import org.testng.Assert.{assertEquals, assertNotNull, assertNull}
import org.testng.annotations.{Test, BeforeMethod}

class ClientFactoryTest extends TestNGSuite {

  /** Since AWS clients are immutable objects and configurations like credential providers cannot be accessed after
   *  client is built, thus we can only do shallow test here to verify if client factory is configured with correct
   *  parameters
   */
  @BeforeMethod
  def setup(): Unit = {
    ClientFactory.skipInitialization = false
  }

  @Test
  def clientFactoryInitializationWithouAssumeRoleArnTest(): Unit = {
    ClientFactory.initialize(region = "us-west-2")
    assertNull(ClientFactory.assumeRoleArn)
    assertNull(ClientFactory.stsAssumeRoleCredentialsProvider)
    assertEquals(ClientFactory.region, "us-west-2")
  }

  @Test
  def clientFactoryInitializationWithAssumeRoleArnTest(): Unit = {
    val testRoleArn = "test-role"

    ClientFactory.initialize(region = "us-west-2", roleArn = testRoleArn)
    assertEquals(ClientFactory.assumeRoleArn, testRoleArn)
    assertNotNull(ClientFactory.stsAssumeRoleCredentialsProvider)
    assertEquals(ClientFactory.region, "us-west-2")
  }
}
