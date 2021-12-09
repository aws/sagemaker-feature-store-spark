package com.amazonaws.services.sagemaker.featurestore.sparksdk.validators

import com.amazonaws.services.sagemaker.featurestore.sparksdk.exceptions.ValidationError
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatestplus.testng.TestNGSuite
import org.testng.Assert.assertEquals
import org.testng.annotations.{DataProvider, Test}
import software.amazon.awssdk.services.sagemaker.model.{DescribeFeatureGroupResponse, FeatureDefinition, FeatureType}

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
  def validateSchemaTest_negative(testDataFrame: DataFrame): Unit = {
    val response = buildTestDescribeFeatureGroupResponse()
    InputDataSchemaValidator.validateSchema(testDataFrame, response)
  }

  @Test(
    dataProvider = "validateSchemaPositiveTestDataProvider"
  )
  def validateSchemaTest_positive(testDataFrame: DataFrame, expectedDataTypeMap: Map[String, String]): Unit = {
    val response        = buildTestDescribeFeatureGroupResponse()
    val validatedSchema = InputDataSchemaValidator.validateSchema(testDataFrame, response)
    for (field <- validatedSchema.schema.fields) {
      assertEquals(field.dataType.typeName, expectedDataTypeMap(field.name))
    }
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
}
