package software.amazon.sagemaker.featurestore.sparksdk

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType

trait SparkRowEncoderAdaptorLike {
  def encoderFor(schema: StructType): ExpressionEncoder[Row]
}