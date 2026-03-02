package software.amazon.sagemaker.featurestore.sparksdk

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.StructType

object SparkRowEncoderAdaptor extends SparkRowEncoderAdaptorLike {
  override def encoderFor(schema: StructType): ExpressionEncoder[Row] = {
    // Spark 3.5+ changed RowEncoder.encoderFor() to return AgnosticEncoder[Row]
    // instead of ExpressionEncoder[Row]. Wrapping with ExpressionEncoder(...)
    // bridges the new encoder back to the type that Dataset.flatMap expects.
    ExpressionEncoder(RowEncoder.encoderFor(schema))
  }
}