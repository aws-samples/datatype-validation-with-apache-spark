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

package com.awsproserve.parser

import com.awsproserve.common.Constant
import org.apache.spark.internal.Logging
import com.awsproserve.parser.DelimitedSchema.{JSONSchemaDataStructure, JSONSchemaRoot}
import play.api.libs.functional.syntax.toFunctionalBuilderOps
import play.api.libs.json.{JsPath, JsValue, Json, Reads}

import scala.collection.mutable.ListBuffer


/**
 * Contains methods to parse input JSON schema
 */
class DelimitedSchema(jsonMultilineString: String) extends Logging {

  var dataStructure: List[JSONSchemaDataStructure] = List[JSONSchemaDataStructure]()

  /**
   * parses JSON schema string to get delimiter
   *
   * @return string delimiter from JSON schema
   */
  def getDelimiter: String = {
    try {
      getJsonRoot.delimiter
    }
    catch {
      case exception: Throwable =>
        logError("could not find delimiter from given JSON schema string")
        logError(exception.getMessage)
        throw exception
    }
  }

  /**
   * parses JSON schema string to get root element
   *
   * @return string Root object from JSON schema
   */
  def getJsonRoot: JSONSchemaRoot = {
    val parsedJson: JsValue = Json.parse(jsonMultilineString)
    val jsonRoot = parsedJson.as[JSONSchemaRoot]
    jsonRoot
  }

  /**
   * parses JSON schema string to get file header
   *
   * @return boolean file header flag from JSON schema
   */
  def getFileHeader: Boolean = {
    try {
      getJsonRoot.fileHeader
    }
    catch {
      case exception: Throwable =>
        logError("could not find delimiter from given JSON schema string")
        logError(exception.getMessage)
        throw exception
    }
  }

  def getQuoteChar: Char = {
      getJsonRoot.quoteChar.getOrElse("\"").toCharArray.charAt(0)
  }

  def getEscapeChar: Char = {
    getJsonRoot.escapeChar.getOrElse("\\").toCharArray.charAt(0)
  }

  def getLineSeparator: String = {
    getJsonRoot.lineSeparator.getOrElse("\n")
  }

  /**
   * parses JSON schema and serializes as list of JSONSchemaDataStructure
   *
   * @return
   */
  def parseSchema: List[JSONSchemaDataStructure] = {

    try {
      val fields: ListBuffer[JSONSchemaDataStructure] = ListBuffer[JSONSchemaDataStructure]()
      val jsonRoot = getJsonRoot
      dataStructure = jsonRoot.dataStructure

      val dataTypes: List[String] = dataStructure.map(field => field.dataType.trim.toUpperCase())
      verifyAcceptedDataType(dataTypes)
      verifyFormatExists(dataStructure)
      dataStructure.foreach(field => {
        val fieldName = field.fieldName.trim
        val dataType = field.dataType.trim.replaceAll(" ", "").toUpperCase()
        val format = field.dataFormat
        val nullable = field.nullable
        fields += JSONSchemaDataStructure(fieldName, dataType, nullable, format)
      })

      fields.toList
    }
    catch {
      case exception: Throwable =>
        logError("could not parse given JSON schema string")
        logError(exception.getMessage)
        throw exception
    }

  }


  /**
   * Verifies if data types from JSON schema are accepted data types
   *
   * @param dataTypes data type to verify
   * @return
   */
  def verifyAcceptedDataType(dataTypes: List[String]): Unit = {
    val isAcceptedDaType = dataTypes.forall(Constant.acceptedDataTypes.contains)
    val unacceptedDataTypes = (Constant.acceptedDataTypes diff dataTypes).mkString(", ")
    val acceptedDataTypes = Constant.acceptedDataTypes.mkString(", ")
    if (!isAcceptedDaType) {
      throw new IllegalArgumentException(f"invalid datatype(s) found: '$unacceptedDataTypes'. valid data types are: '$acceptedDataTypes'")
    }
  }

  /**
   * Verifies if format element exists in JSON schema for required data types
   *
   * @param dataStructure data structure object
   */
  def verifyFormatExists(dataStructure: List[JSONSchemaDataStructure]): Unit = {
    val requireFormatForDataTypes = Constant.requireFormatForDataTypes
    val requireFormatForDataTypesString = Constant.requireFormatForDataTypes.mkString(", ")

    try {
      dataStructure.filter(field => requireFormatForDataTypes.contains(field.dataType.trim.toUpperCase())).foreach(field => {
        if (field.dataFormat.getOrElse("").trim.eq("")) {
          throw new IllegalArgumentException(f"format not found for field: '${field.fieldName}', format is required for data types: '$requireFormatForDataTypesString'")
        }

        if (field.dataType.trim.toUpperCase().eq("DECIMAL")) {
          val regexPatternDecimalDataTypeFormat = """DECIMAL\(\d+,\d+\)""".r
          val dataFormat = field.dataFormat.getOrElse("").trim.replaceAll(" ", "")
          val dt = field.dataType.trim.toUpperCase
          val dataType = f"$dt($dataFormat)"
          val matches = regexPatternDecimalDataTypeFormat.findAllIn(f"$dataType($dataFormat)").toList

          // check if decimal(Prevision,Scale) pattern matches
          if (matches.isEmpty) {
            throw new IllegalArgumentException(f"invalid DECIMAL data type format found '$dataType'")
          }

          val precision = dataFormat.split(",")(0).toInt
          val scale = dataFormat.split(",")(1).toInt

          if (precision < scale) {
            throw new IllegalArgumentException(f"invalid DECIMAL data type format found '$dataType'")
          }
        }
      })
    } catch {
      case exception: Throwable =>
        logError(exception.getMessage)
        throw exception
    }
  }

  // parse using case class JSONSchemaDataStructure
  implicit val jsonDataStructure: Reads[JSONSchemaDataStructure] = (
    (JsPath \ "fieldName").read[String] and
      (JsPath \ "dataType").read[String] and
      (JsPath \ "nullable").read[Boolean] and
      (JsPath \ "dataFormat").readNullable[String]
    ) (JSONSchemaDataStructure.apply _)

  // parse using case class JSONSchemaRoot
  implicit val jsonSchemaRoot: Reads[JSONSchemaRoot] = (
    (JsPath \ "schemaVersion").read[String] and
      (JsPath \ "dataStructure").read[List[JSONSchemaDataStructure]] and
      (JsPath \ "delimiter").read[String] and
      (JsPath \ "quoteChar").readNullable[String] and
      (JsPath \ "escapeChar").readNullable[String] and
      (JsPath \ "lineSeparator").readNullable[String] and
      (JsPath \ "fileHeader").read[Boolean]
    ) (JSONSchemaRoot.apply _)

}

object DelimitedSchema {
  /**
   * case class to define dataStructure JSON element of schema
   *
   * @param fieldName  field name
   * @param dataType   field data type
   * @param nullable   field nullability
   * @param dataFormat field data type format
   */
  case class JSONSchemaDataStructure(fieldName: String,
                                     dataType: String,
                                     nullable: Boolean,
                                     dataFormat: Option[String]
                                    )

  // case class to define Root JSON element of schema

  /**
   * case class to define Root JSON element of schema
   *
   * @param schemaVersion schema version number
   * @param dataStructure JSONSchemaDataStructure object list
   */
  case class JSONSchemaRoot(
                             schemaVersion: String,
                             dataStructure: List[JSONSchemaDataStructure],
                             delimiter: String,
                             quoteChar: Option[String],
                             escapeChar: Option[String],
                             lineSeparator: Option[String],
                             fileHeader: Boolean
                           )
}