package software.amazon.sagemaker.featurestore.sparksdk.validators

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatestplus.testng.TestNGSuite
import org.testng.Assert.assertEquals
import org.testng.annotations.{DataProvider, Test}
import software.amazon.awssdk.services.sagemaker.model.{DescribeFeatureGroupResponse, FeatureDefinition, FeatureType}
import software.amazon.sagemaker.featurestore.sparksdk.exceptions.ValidationError

import scala.Double.NaN

class InputDataSchemaValidatorTest extends TestNGSuite {

  private final val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("TestProgram")
    .master("local")
    .getOrCreate()
  import sparkSession.implicits._

  @Test(
    expectedExceptions = Array(classOf[ValidationError]),
    dataProvider = "validateSchemaNegativeTestDataProvider"
  )
  def validateDataFrameTest_negative(testDataFrame: DataFrame): Unit = {
    val response = buildTestDescribeFeatureGroupResponse()
    InputDataSchemaValidator.validateInputDataFrame(testDataFrame, response)
  }

  @Test(
    expectedExceptions = Array(classOf[ValidationError]),
    expectedExceptionsMessageRegExp = "Cannot proceed. Event time feature missing in DataFrame columns: '.*'."
  )
  def validateNoEventtimeDataFrameFailWithMsg(): Unit = {
    val response = buildTestDescribeFeatureGroupResponse()
    val df = Seq(("identifier-1", "0.005", "test-feature", "100"))
      .toDF("record-identifier", "feature-fractional", "feature-string", "feature-integral")
    InputDataSchemaValidator.validateInputDataFrame(df, response)
  }

  @Test(
    dataProvider = "validateSchemaPositiveTestDataProvider"
  )
  def validateDataFrameTest_positive(testDataFrame: DataFrame): Unit = {
    val response = buildTestDescribeFeatureGroupResponse()
    InputDataSchemaValidator.validateInputDataFrame(testDataFrame, response)
  }

  @Test(
    dataProvider = "transformDataFrameTypeTestDataProvider"
  )
  def transformDataFrameTypeTest(testDataFrame: DataFrame, expectedDataTypeMap: Map[String, String]): Unit = {
    val response             = buildTestDescribeFeatureGroupResponse()
    val transformedDataFrame = InputDataSchemaValidator.transformDataFrameType(testDataFrame, response)
    for (field <- transformedDataFrame.schema.fields) {
      assertEquals(field.dataType.typeName, expectedDataTypeMap(field.name))
    }
  }

  @Test(
    dataProvider = "validStrEventTimeProvider"
  )
  def validateStrEventTimeFormatTest_positive(testDataFrame: DataFrame): Unit = {
    val response = buildTestDescribeFeatureGroupResponseStrEventtime()
    InputDataSchemaValidator.validateInputDataFrame(testDataFrame, response)
  }

  @Test(
    expectedExceptions = Array(classOf[ValidationError]),
    dataProvider = "invalidStrEventTimeProvider"
  )
  def validateStrEventTimeFormatTest_negative(testDataFrame: DataFrame): Unit = {
    val response = buildTestDescribeFeatureGroupResponseStrEventtime()
    InputDataSchemaValidator.validateInputDataFrame(testDataFrame, response)
  }

  @DataProvider
  def validStrEventTimeProvider(): Array[Array[Any]] = {
    Array(
      Array(
        Seq(("identifier-1", "2022-02-15T00:03:30Z"))
          .toDF("record-identifier", "event-time")
      ),
      Array(
        Seq(("identifier-1", "2022-02-15T00:03:30.932Z"))
          .toDF("record-identifier", "event-time")
      )
    )
  }

  @DataProvider
  def invalidStrEventTimeProvider(): Array[Array[Any]] = {
    Array(
      // time malformat
      Array(
        Seq(("identifier-1", "2022-02-15T00:03:30+0000"))
          .toDF("record-identifier", "event-time")
      ),
      Array(
        Seq(("identifier-1", "2022-02-15T00:03:30-08:00"))
          .toDF("record-identifier", "event-time")
      ),
      Array(
        Seq(("identifier-1", "2022-02-15T00:03:30-08"))
          .toDF("record-identifier", "event-time")
      ),
      Array(
        Seq(("identifier-1", "2022-02-15T00:03:30"))
          .toDF("record-identifier", "event-time")
      ),
      Array(
        Seq(("identifier-1", "2022-02-15T00:03:30.931"))
          .toDF("record-identifier", "event-time")
      ),
      Array(
        Seq(("identifier-1", "2022-02-15 00:03:30Z"))
          .toDF("record-identifier", "event-time")
      ),
      Array(
        Seq(("identifier-1", "2022/02/15T00:03:30Z"))
          .toDF("record-identifier", "event-time")
      ),
      Array(
        Seq(("identifier-1", "2022-02-15"))
          .toDF("record-identifier", "event-time")
      ),
      // time out of range
      Array(
        Seq(("identifier-1", "2022-02-15T25:03:30.932Z"))
          .toDF("record-identifier", "event-time")
      ),
      Array(
        Seq(("identifier-1", "2022-22-15T05:03:30.932Z"))
          .toDF("record-identifier", "event-time")
      )
    )
  }

  @DataProvider
  def validateSchemaNegativeTestDataProvider(): Array[Array[Any]] = {
    Array(
      // Column names contain invalid char
      Array(Seq(("identifier")).toDF("{record-identifier}")),
      // Columns contain unknow feature
      Array(Seq(("feature")).toDF("feature-unknown")),
      // Missing required feature name in the data frame
      Array(Seq(("identifier-1")).toDF("record-identifier")),
      Array(Seq(("1631091971")).toDF("event-time")),
      // Columns contain reserved feature names
      Array(
        Seq(("identifier-1", "1631091971", "true"))
          .toDF("record-identifier", "event-time", "is_deleted")
      ),
      // Invalid feature values
      Array(
        Seq(("identifier-1", "invalid-event-time"))
          .toDF("record-identifier", "event-time")
      ),
      Array(
        Seq(("identifier-1", "1631091971", "invalid-integral"))
          .toDF("record-identifier", "event-time", "feature-integral")
      ),
      Array(
        Seq(("identifier-1", "1631091971", "invalid-fractional"))
          .toDF("record-identifier", "event-time", "feature-fractional")
      ),
      Array(
        Seq(("identifier-1", NaN))
          .toDF("record-identifier", "event-time")
      ),
      Array(
        Seq((null, "1631091971"))
          .toDF("record-identifier", "event-time")
      ),
      Array(
        Seq(("identifier-1", null))
          .toDF("record-identifier", "event-time")
      )
    )
  }

  @DataProvider
  def validateSchemaPositiveTestDataProvider(): Array[Array[Any]] = {
    Array(
      Array(
        Seq(("identifier-1", "1631091971", "0.005", "test-feature", "100"))
          .toDF("record-identifier", "event-time", "feature-fractional", "feature-string", "feature-integral")
      )
    )
  }

  @DataProvider
  def transformDataFrameTypeTestDataProvider(): Array[Array[Any]] = {
    Array(
      Array(
        Seq(("identifier-1", "1631091971", "0.005", "test-feature", "100"))
          .toDF("record-identifier", "event-time", "feature-fractional", "feature-string", "feature-integral"),
        Map(
          "record-identifier"  -> "string",
          "event-time"         -> "double",
          "feature-fractional" -> "double",
          "feature-string"     -> "string",
          "feature-integral"   -> "long"
        )
      )
    )
  }

  def buildTestDescribeFeatureGroupResponse(): DescribeFeatureGroupResponse = {
    DescribeFeatureGroupResponse
      .builder()
      .recordIdentifierFeatureName("record-identifier")
      .eventTimeFeatureName("event-time")
      .featureDefinitions(
        FeatureDefinition
          .builder()
          .featureName("record-identifier")
          .featureType(FeatureType.STRING)
          .build(),
        FeatureDefinition
          .builder()
          .featureName("event-time")
          .featureType(FeatureType.FRACTIONAL)
          .build(),
        FeatureDefinition
          .builder()
          .featureName("feature-string")
          .featureType(FeatureType.STRING)
          .build(),
        FeatureDefinition
          .builder()
          .featureName("feature-integral")
          .featureType(FeatureType.INTEGRAL)
          .build(),
        FeatureDefinition
          .builder()
          .featureName("feature-fractional")
          .featureType(FeatureType.FRACTIONAL)
          .build()
      )
      .build()
  }

  def buildTestDescribeFeatureGroupResponseStrEventtime(): DescribeFeatureGroupResponse = {
    DescribeFeatureGroupResponse
      .builder()
      .recordIdentifierFeatureName("record-identifier")
      .eventTimeFeatureName("event-time")
      .featureDefinitions(
        FeatureDefinition
          .builder()
          .featureName("record-identifier")
          .featureType(FeatureType.STRING)
          .build(),
        FeatureDefinition
          .builder()
          .featureName("event-time")
          .featureType(FeatureType.STRING)
          .build()
      )
      .build()
  }
}
