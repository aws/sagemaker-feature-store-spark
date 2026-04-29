package software.amazon.sagemaker.featurestore.sparksdk.helpers

/** Build-time gate for features that require a minimum Spark version.
 *
 *  Two implementations live in the Spark-version-specific source directories (`scala-spark-3.5` /
 *  `scala-spark-3.1-3.4`). `requireSparkVersion(major, minor)` throws `UnsupportedOperationException` if the requested
 *  minimum version is greater than the Spark version this SDK jar was built against; otherwise it is a no-op.
 */
trait MinSparkVersionGateLike {

  /** Throws if the requested minimum Spark version is greater than this build's Spark version. */
  def requireSparkVersion(major: Int, minor: Int): Unit
}
