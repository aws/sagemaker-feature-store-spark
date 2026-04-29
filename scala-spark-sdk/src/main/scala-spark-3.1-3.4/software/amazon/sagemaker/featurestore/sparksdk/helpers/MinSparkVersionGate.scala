package software.amazon.sagemaker.featurestore.sparksdk.helpers

import scala.math.Ordering.Implicits._

/** Spark 3.1-3.4 build: allows features requiring Spark 3.4 or older; throws for anything newer. */
object MinSparkVersionGate extends MinSparkVersionGateLike {
  private val builtFor = (3, 4)

  def requireSparkVersion(major: Int, minor: Int): Unit =
    if ((major, minor) > builtFor)
      throw new UnsupportedOperationException(
        s"Feature requires Spark $major.$minor+; this SDK jar was built against Spark <=3.4."
      )
}
