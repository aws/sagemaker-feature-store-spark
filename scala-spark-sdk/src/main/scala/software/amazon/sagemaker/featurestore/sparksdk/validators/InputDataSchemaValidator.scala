/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *      http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 */

package software.amazon.sagemaker.featurestore.sparksdk.validators

import org.apache.spark.sql.functions.{col, concat_ws, lit, when}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame}
import software.amazon.awssdk.services.sagemaker.model.{DescribeFeatureGroupResponse, FeatureDefinition}
import software.amazon.sagemaker.featurestore.sparksdk.exceptions.ValidationError

import java.util.regex.Pattern
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/** An object includes methods to detect schema errors of input DataFrame.
 */
object InputDataSchemaValidator {

  val RESERVED_FEATURE_NAMES: Set[String] =
    Set("is_deleted", "write_time", "api_invocation_time", "year", "month", "day", "hour", "temp_event_time_col")
  val TYPE_MAP: Map[String, String] =
    Map("Integral" -> "long", "String" -> "string", "Fractional" -> "double")

  /** Validate input Spark DataFrame.
   *
   *  @param dataFrame
   *    input Spark DataFrame to be ingested.
   *  @param describeResponse
   *    response of DescribeFeatureGroup
   */
  def validateInputDataFrame(
      dataFrame: DataFrame,
      describeResponse: DescribeFeatureGroupResponse
  ): DataFrame = {
    val recordIdentifierName = describeResponse.recordIdentifierFeatureName()
    val eventTimeFeatureName = describeResponse.eventTimeFeatureName()

    validateSchemaNames(dataFrame.schema.names, describeResponse, recordIdentifierName, eventTimeFeatureName)

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

  def validateSchemaNames(
      schemaNames: Array[String],
      describeResponse: DescribeFeatureGroupResponse,
      recordIdentifierName: String,
      eventTimeFeatureName: String
  ): Unit = {
    val invalidCharSet              = "[,;{}()\n\t=]"
    val invalidCharSetPattern       = Pattern.compile(invalidCharSet)
    val unknown_columns             = ListBuffer[String]()
    var missingRequiredFeatureNames = Set(recordIdentifierName, eventTimeFeatureName)

    val features = describeResponse
      .featureDefinitions()
      .asScala
      .toStream
      .map(feature => feature.featureName())
      .toSet

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
    val lambdaCreator = (sparkType: String) => (featureName: String) => col(featureName).cast(sparkType)

    val conversionsMap = featureDefinitions.foldLeft(Map.empty[String, String => Column]) {
      (resultMap, featureDefinition) =>
        {
          val featureName = featureDefinition.featureName()
          val sparkType   = TYPE_MAP(featureDefinition.featureTypeAsString())
          if (featureName.equals(eventTimeFeatureName)) {
            resultMap + (eventTimeFeatureName -> ((featureName: String) =>
              col(featureName).cast(sparkType).cast(TimestampType)
            ))
          } else if (dataFrame.schema.names.contains(featureName)) {
            resultMap + (featureName -> lambdaCreator(sparkType))
          } else {
            resultMap
          }
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
  ): List[Column] = {
    // Mark the row as not valid if:
    // 1. The data cannot be casted to the type specified in feature definition
    // 2. The value of data is NaN
    // 3. Feature value of event time feature is null
    // 4. Feature value of record identifier is null
    dataTypeValidatorMap.foldLeft(List[Column]()) { case (resultList, (featureName, conversion)) =>
      if (featureName == recordIdentifierName || featureName == eventTimeFeatureName) {
        resultList :+ when(
          conversion(featureName).isNull && col(featureName).isNotNull
            || col(featureName).isNaN
            || col(featureName).isNull,
          lit(featureName + " not valid")
        ).otherwise(lit(null))
      } else {
        resultList :+ when(
          conversion(featureName).isNull && col(featureName).isNotNull
            || col(featureName).isNaN,
          lit(featureName + " not valid")
        ).otherwise(lit(null))
      }
    }
  }
}
