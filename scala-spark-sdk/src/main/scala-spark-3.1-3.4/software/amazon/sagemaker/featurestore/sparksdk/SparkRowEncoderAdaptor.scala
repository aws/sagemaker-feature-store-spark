package software.amazon.sagemaker.featurestore.sparksdk

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.StructType

object SparkRowEncoderAdaptor extends SparkRowEncoderAdaptorLike {
  override def encoderFor(schema: StructType): ExpressionEncoder[Row] = {
    // For Spark < 3.5, RowEncoder(schema) is the API
    RowEncoder(schema)
  }
}