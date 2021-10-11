import com.amazonaws.services.sagemaker.featurestore.sparksdk.FeatureStoreManager
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ProgramOffline {
  def main(args: Array[String]): Unit = {

    /* Create a Spark session */
    val spark = SparkSession.builder().appName("TestProgram").getOrCreate()

    /* Build the data */
    val data = List(
      Row("2021-07-01T12:20:12Z"),
      Row("2021-07-02T12:20:13Z"),
      Row("2021-07-03T12:20:14Z"),
      Row("2021-07-04T12:20:16Z")
    )
    val schema = StructType(
      List(StructField("potato", StringType, nullable = true))
    )

    /* Convert list to RDD */
    val rdd = spark.sparkContext.parallelize(data)

    /* Create data frame */
    val df = spark.createDataFrame(rdd, schema)

    /* Batch Ingestion */
    val featureGroupArn =
      "arn:aws:sagemaker:us-west-2:317650540784:feature-group/sucan-test-feature-group-offline-only-kms"
    val featureStoreBatchDataIngestor = new FeatureStoreManager()
    spark.time {
      featureStoreBatchDataIngestor.ingestData(df, featureGroupArn, directOfflineStore = true)
    }
  }
}
