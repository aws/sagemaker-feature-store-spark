package software.amazon.sagemaker.featurestore.sparksdk.helpers

import org.scalatestplus.testng.TestNGSuite
import org.testng.Assert.{assertTrue, fail}
import org.testng.annotations.Test

/** On Spark 3.1-3.4 builds, requireSparkVersion(3, 5) must throw; requireSparkVersion(3, 4) must be a no-op. */
class MinSparkVersionGateTest extends TestNGSuite {

  @Test
  def requireSparkVersion35ThrowsOnOldSparkBuildTest(): Unit = {
    try {
      MinSparkVersionGate.requireSparkVersion(3, 5)
      fail(
        "Expected UnsupportedOperationException from MinSparkVersionGate.requireSparkVersion(3, 5) on Spark 3.1-3.4 build"
      )
    } catch {
      case e: UnsupportedOperationException =>
        assertTrue(e.getMessage.contains("3.5"), s"Message should contain '3.5': ${e.getMessage}")
        assertTrue(e.getMessage.contains("3.4"), s"Message should contain '3.4': ${e.getMessage}")
    }
  }

  @Test
  def requireSparkVersion34IsNoopOnOldSparkBuildTest(): Unit = {
    MinSparkVersionGate.requireSparkVersion(3, 4)
  }
}
