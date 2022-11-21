package software.amazon.sagemaker.featurestore.sparksdk.exceptions

/** Throw if any of the record fails online ingestion
 *
 *  @param message
 *    Message describing the failure details.
 */
case class StreamIngestionFailureException(message: String) extends BaseException(message)
