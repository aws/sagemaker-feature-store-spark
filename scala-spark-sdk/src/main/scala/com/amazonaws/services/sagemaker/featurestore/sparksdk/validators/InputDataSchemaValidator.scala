package com.amazonaws.services.sagemaker.featurestore.sparksdk.validators

import com.amazonaws.services.sagemaker.featurestore.sparksdk.exceptions.ValidationError
import org.apache.spark.sql.functions.{col, concat_ws, lit, when}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame}
import software.amazon.awssdk.services.sagemaker.model.{DescribeFeatureGroupResponse, FeatureDefinition}

import java.util.regex.Pattern
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * An object includes methods to detect schema errors of input DataFrame.
  */
object InputDataSchemaValidator {

  val RESERVED_FEATURE_NAMES: Set[String] = Set("is_deleted", "write_time", "api_invocation_time")
  val TYPE_MAP: Map[String, String] =
    Map("Integral" -> "long", "String" -> "string", "Fractional" -> "double")

  /**
    * Validate input Spark DataFrame.
    *
    * @param dataFrame input Spark DataFrame to be ingested.
    * @param describeResponse response of DescribeFeatureGroup
    */
  def validateSchema(
      dataFrame: DataFrame,
      describeResponse: DescribeFeatureGroupResponse
  ): DataFrame = {
    val recordIdentifierName = describeResponse.recordIdentifierFeatureName()
    val eventTimeFeatureName = describeResponse.eventTimeFeatureName()

    val featuresInFeatureGroup = describeResponse
      .featureDefinitions()
      .asScala
      .toStream
      .map(feature => feature.featureName())
      .toSet
      .asJava

    validateSchemaNames(dataFrame.schema.names, featuresInFeatureGroup, recordIdentifierName, eventTimeFeatureName)

    // Numeric data types validation - For example, verify that only numeric values that are within bounds
    // of the Integer and Double data types are present in the corresponding fields.
    // This should be caught by the type conversion check as well
    val schemaDataTypeValidatorMap = getSchemaDataTypeValidatorMap(
      dataFrame = dataFrame,
      featureDefinitions = describeResponse.featureDefinitions().asScala.toList,
      describeResponse.eventTimeFeatureName()
    )
    val schemaDataTypeValidatorColumn = getSchemaDataTypeValidatorColumn(
      schemaDataTypeValidatorMap,
      recordIdentifierName,
      eventTimeFeatureName
    )

    val invalidRows = dataFrame
      .withColumn(
        "dataTypeValidationErrors",
        concat_ws(",", schemaDataTypeValidatorColumn: _*)
      )
      .filter(col("dataTypeValidationErrors").like("%not valid"))

    if (!invalidRows.isEmpty) {

      invalidRows
        .select(col(recordIdentifierName), col("dataTypeValidationErrors"))
        .show(numRows = 20, truncate = false)

      throw ValidationError(
        "Cannot proceed. Some records contain columns with data types that are not registered in the FeatureGroup " +
          "or records values equal to NaN."
      )
    }

    val dataTypeTransformationMap = getSchemaDataTypeTransformationMap(
      schemaDataTypeValidatorMap,
      describeResponse.featureDefinitions().asScala.toList,
      eventTimeFeatureName
    )

    dataFrame.select(dataFrame.columns.map { col =>
      dataTypeTransformationMap(col).apply(col)
    }: _*)
  }

  private def validateSchemaNames(
      schemaNames: Array[String],
      features: java.util.Set[String],
      recordIdentifierName: String,
      eventTimeFeatureName: String
  ): Unit = {
    val invalidCharSet              = "[,;{}()\n\t=]"
    val invalidCharSetPattern       = Pattern.compile(invalidCharSet)
    val unknown_columns             = ListBuffer[String]()
    var missingRequiredFeatureNames = Set(recordIdentifierName, eventTimeFeatureName)

    for (name <- schemaNames) {
      // Verify there are no invalid characters ",;{}()\n\t=" in the schema names.
      if (invalidCharSetPattern.matcher(name).matches()) {
        throw ValidationError(
          s"Cannot proceed. Invalid char among '$invalidCharSet' detected in '$name'."
        )
      }

      // Verify there is no reserved feature name.
      if (RESERVED_FEATURE_NAMES.contains(name)) {
        throw ValidationError(
          s"Cannot proceed. Detected column with reserved feature name '$name'."
        )
      }

      if (!features.contains(name)) {
        unknown_columns += name
      }

      if (missingRequiredFeatureNames.contains(name)) {
        missingRequiredFeatureNames -= name
      }
    }

    // Verify there is no unknown column.
    if (unknown_columns.nonEmpty) {
      throw ValidationError(
        s"Cannot proceed. Schema contains unknown columns: '$unknown_columns'"
      )
    }

    // Verify all required feature names are present in schema.
    if (missingRequiredFeatureNames.nonEmpty) {
      throw ValidationError(
        s"Cannot proceed. Missing feature names '$missingRequiredFeatureNames' in schema."
      )
    }
  }

  private def getSchemaDataTypeValidatorMap(
      dataFrame: DataFrame,
      featureDefinitions: List[FeatureDefinition],
      eventTimeFeatureName: String
  ): Map[String, String => Column] = {
    var conversionsMap: Map[String, String => Column] = Map()

    val lambdaCreator = (sparkType: String) => (featureName: String) => col(featureName).cast(sparkType)

    for (featureDefinition <- featureDefinitions) {
      val featureName = featureDefinition.featureName()
      val sparkType   = TYPE_MAP(featureDefinition.featureTypeAsString())
      if (featureName.equals(eventTimeFeatureName)) {
        conversionsMap += eventTimeFeatureName -> ((featureName: String) =>
          col(featureName).cast(sparkType).cast(TimestampType)
        )
      } else if (dataFrame.schema.names.contains(featureName)) {
        conversionsMap += (featureName -> lambdaCreator(sparkType))
      }
    }
    conversionsMap
  }

  private def getSchemaDataTypeTransformationMap(
      validatorMap: Map[String, String => Column],
      featureDefinitions: List[FeatureDefinition],
      eventTimeFeatureName: String
  ): Map[String, String => Column] = {
    val eventTimeFeatureType =
      featureDefinitions.find(feature => feature.featureName().equals(eventTimeFeatureName)).get.featureTypeAsString()
    var dataTypeTransformationMap = validatorMap
    dataTypeTransformationMap += (eventTimeFeatureName -> ((featureName: String) =>
      col(featureName).cast(TYPE_MAP(eventTimeFeatureType))
    ))

    dataTypeTransformationMap
  }

  private def getSchemaDataTypeValidatorColumn(
      dataTypeValidatorMap: Map[String, String => Column],
      recordIdentifierName: String,
      eventTimeFeatureName: String
  ): Seq[Column] = {
    var dataTypeValidatorColumnArray = Seq[Column]()

    // Mark the row as not valid if:
    // 1. The data cannot be casted to the type specified in feature definition
    // 2. The value of data is NaN
    // 3. Feature value of event time feature is null
    // 4. Feature value of record identifier is null
    for ((featureName, conversion) <- dataTypeValidatorMap) {
      dataTypeValidatorColumnArray = dataTypeValidatorColumnArray :+ when(
        conversion(featureName).isNull && col(featureName).isNotNull
          || col(featureName).isNaN
          || col(recordIdentifierName).isNull
          || col(eventTimeFeatureName).isNull,
        lit(featureName + " not valid")
      ).otherwise(lit(null))
    }

    dataTypeValidatorColumnArray
  }
}
