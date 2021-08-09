/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.awsproserve.validation

import com.awsproserve.common.{Constant, Utils}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.format.DateTimeFormat
import com.awsproserve.parser.DelimitedSchema.JSONSchemaDataStructure
import com.awsproserve.parser.{DelimitedSchema, DelimitedText}

import scala.collection.mutable.ListBuffer


/**
 * Delimited data com.awsproserve.validation
 */
object Delimited extends Logging {

  /**
   *
   * @param spark               spark session
   * @param jsonMultilineString json metadata string
   * @param file                input file path
   * @return
   */
  def validate(spark: SparkSession, jsonMultilineString: String, file: String): DataFrame = {
    // Parse given JSON schema
    val schemaParser = new DelimitedSchema(jsonMultilineString)
    var schema: List[JSONSchemaDataStructure] = List[JSONSchemaDataStructure]()

    val obj = new DelimitedText(delimiter = schemaParser.getDelimiter.toCharArray.charAt(0),
      quoteChar = schemaParser.getQuoteChar,
      escapeChar = schemaParser.getEscapeChar,
      lineSeparator = schemaParser.getLineSeparator,
      ignoreLeadingWhiteSpace = true,
      ignoreTrailingWhiteSpace = true,
      headerExtractionEnabled = schemaParser.getFileHeader
    )
    try {
      schema = schemaParser.parseSchema
    }
    catch {
      case exception: Throwable =>
        logError("could not parse given metadata JSON")
        throw exception
    }

    val rdd: RDD[Array[String]] = obj.parse(spark = spark, fileLocation = file)
    val allRecordDf: DataFrame = validateInvalidColumnLength(spark = spark, rdd = rdd, schema = schema, delimiter = schemaParser.getDelimiter.toCharArray.charAt(0))
    val invalidColumnLengthCheckFailDf = allRecordDf.filter(col("error_flag") > 0)
    val dataTypeValidationToDoDf = allRecordDf.filter(col("error_flag") === 0)
    val dataTypeValidatedDf = validateDataTypes(spark = spark, dataFrame = dataTypeValidationToDoDf, schema = schema)

    val stringDataTypeDf = dataTypeValidatedDf.union(invalidColumnLengthCheckFailDf)
    typeCastDataFrame(schema = schema, dataFrame = stringDataTypeDf)
  }

  /**
   * Validates if number of columns in data matches expected number of columns
   * @param spark     spark session
   * @param rdd       rdd to validate
   * @param schema    JSON metadata
   * @param delimiter field delimiter
   * @return
   */
  def validateInvalidColumnLength(spark: SparkSession, rdd: RDD[Array[String]], schema: List[JSONSchemaDataStructure], delimiter: Char): DataFrame = {

    val fieldNames = schema.map(_.fieldName)
    val dataFrameSchema = getDataFrameSchema(fieldNames)
    val expectedNumColumns = schema.length

    val nullRow: Array[String] = new Array[String](expectedNumColumns)
    // For each array element in the rdd

    val outputRdd: RDD[Row] = rdd.map(
      record => {
        var errorFlag = 0
        val errorMessageStruct = ListBuffer[Map[String, String]]()
        val numOfColumns = record.length

        // If actual number of columns are more than expected, set error flag to 2
        if (numOfColumns < expectedNumColumns) {
          errorFlag = 2
          errorMessageStruct += Map("record" -> record.mkString(delimiter.toString), "expectedNumberOfColumns" -> expectedNumColumns.toString, "actualNumberOfColumns" -> numOfColumns.toString )
          Row.fromSeq(nullRow.toSeq ++ Array[Any](errorFlag, errorMessageStruct))
        }
        else if (numOfColumns > expectedNumColumns) {
          errorFlag = 3
          errorMessageStruct += Map("record" -> record.mkString(delimiter.toString), "expectedNumberOfColumns" -> expectedNumColumns.toString, "actualNumberOfColumns" -> numOfColumns.toString )
          Row.fromSeq(nullRow.toSeq ++ Array[Any](errorFlag, errorMessageStruct))
        }
        else {
          Row.fromSeq(record.toSeq ++ Array[Any](errorFlag, errorMessageStruct))
        }
      })
    spark.createDataFrame(outputRdd, StructType(dataFrameSchema))
  }

  /**
   *
   * @param spark     spark session
   * @param dataFrame dataframe to validate
   * @param schema    schema object
   * @return
   */
  def validateDataTypes(spark: SparkSession, dataFrame: DataFrame, schema: List[JSONSchemaDataStructure]): DataFrame = {
    val fieldNames = schema.map(_.fieldName)
    val dataTypes = schema.map(_.dataType)
    val dataFormat = schema.map(_.dataFormat)
    val nullable = schema.map(_.nullable)
    var stringFieldValue: String = ""
    val dataFrameSchema = getDataFrameSchema(fieldNames)

    val rdd = dataFrame.rdd.map(
      record => {
        var outputRecord: Row = Row()
        var errorFlag = 0
        val errorMessageStruct = ListBuffer[Map[String, String]]()

        for (fieldIndex <- schema.indices) {
          stringFieldValue = record.getString(fieldIndex)

          // Replace comma with empty string for numeric data types
          if (List("INT", "BIGINT", "DOUBLE").contains(dataTypes(fieldIndex)) && stringFieldValue != null) {
            stringFieldValue = Utils.trimString(toReplace = ",", fromReplace = record.getString(fieldIndex))
          }

          if (!Constant.validateMethodMap(dataTypes(fieldIndex))(record.getString(fieldIndex), dataFormat(fieldIndex).getOrElse(""), nullable(fieldIndex))) {
            errorFlag = 1
            val errorMessage = Map(
              "field" -> fieldNames(fieldIndex),
              "value" -> record.getString(fieldIndex),
              "dataType" -> dataTypes(fieldIndex),
              "dataFormat" -> dataFormat(fieldIndex).getOrElse(""),
              "nullability" -> nullable(fieldIndex).toString
            )
            errorMessageStruct += errorMessage
          }

          if (List("date", "timestamp").contains(dataTypes(fieldIndex).toLowerCase)) {
            val patternMap = Map("date" -> "yyyy-MM-dd", "timestamp" -> "yyyy-MM-dd HH:mm:ss")
            try {
              stringFieldValue = DateTimeFormat.forPattern(dataFormat(fieldIndex).getOrElse("")).parseDateTime(stringFieldValue).toString(patternMap(dataTypes(fieldIndex).toLowerCase))
            }
            catch {
              case _: Exception =>
                stringFieldValue = null //"0001-01-01 00"
            }
          }
          outputRecord = Row.fromSeq(outputRecord.toSeq ++ Array[Any](stringFieldValue))
        } // column loop ends here

        // Row.fromSeq(outputRecord.toSeq ++ Array[Any](errorFlag, Json.stringify(Json.toJson(Map("errorFields" -> errorMessageStruct)))))
        Row.fromSeq(outputRecord.toSeq ++ Array[Any](errorFlag, errorMessageStruct.toList))
      } // row loop ends here
    )
    val stringDf = spark.createDataFrame(rdd, StructType(dataFrameSchema))
    typeCastDataFrame(schema = schema, dataFrame = stringDf)
  }

  /**
   *
   * @param fieldNames field names
   * @return
   */
  def getDataFrameSchema(fieldNames: List[String]): StructType = {
    val schemaFields: ListBuffer[StructField] = fieldNames.map(field => StructField(field, StringType)).to[ListBuffer]
    schemaFields += StructField("error_flag", IntegerType)
    schemaFields += StructField("error_message", ArrayType(MapType(StringType, StringType)))
    StructType(schemaFields.toList)
  }

  /**
   * type casts string type dataframe to expected data types
   *
   * @param schema    schema object
   * @param dataFrame dataframe to type cast
   * @return
   */
  def typeCastDataFrame(schema: List[JSONSchemaDataStructure], dataFrame: DataFrame): DataFrame = {
    val sqlExpression: ListBuffer[String] = ListBuffer[String]()
    schema.foreach(field => {

      if (field.dataType == "DECIMAL") {
        val precision = field.dataFormat.getOrElse("").split(",")(0)
        val scale = field.dataFormat.getOrElse("").split(",")(1)
        sqlExpression += f"""cast(${field.fieldName} as ${field.dataType}($precision,$scale)) as ${field.fieldName}"""
      }
      else {
        sqlExpression += f"""cast(${field.fieldName} as ${field.dataType}) as ${field.fieldName}"""
      }
    })
    sqlExpression += "error_flag"
    sqlExpression += "error_message"
    dataFrame.selectExpr(sqlExpression: _*)
  }
}
