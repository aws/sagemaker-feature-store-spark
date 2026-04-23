package software.amazon.sagemaker.featurestore.sparksdk.helpers

import org.scalatestplus.testng.TestNGSuite
import org.testng.Assert.{assertTrue, fail}
import org.testng.annotations.Test

/** On Spark 3.5 builds, requireSparkVersion(3, 5) must be a no-op; requiring a newer version must throw. */
class MinSparkVersionGateTest extends TestNGSuite {

  @Test
  def requireSparkVersion35IsNoopOnSpark35BuildTest(): Unit = {
    MinSparkVersionGate.requireSparkVersion(3, 5)
  }

  @Test
  def requireSparkVersion36ThrowsOnSpark35BuildTest(): Unit = {
    try {
      MinSparkVersionGate.requireSparkVersion(3, 6)
      fail(
        "Expected UnsupportedOperationException from MinSparkVersionGate.requireSparkVersion(3, 6) on Spark 3.5 build"
      )
    } catch {
      case e: UnsupportedOperationException =>
        assertTrue(e.getMessage.contains("3.6"), s"Message should contain '3.6': ${e.getMessage}")
        assertTrue(e.getMessage.contains("3.5"), s"Message should contain '3.5': ${e.getMessage}")
    }
  }
}
