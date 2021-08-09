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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.Decimal
import org.joda.time.format.DateTimeFormat
import com.awsproserve.common.Utils

import java.math.BigInteger

/**
 * Data type com.awsproserve.validation methods
 */
object DataType extends Logging {

  /**
   *
   * @param value      literal value string to validate
   * @param isNullable true for nullable, false for not nullable
   * @return true if valid, false if invalid
   * @return
   */
  def isInteger(value: String, format: String = "", isNullable: Boolean): Boolean = {
    var valid = true
    logDebug(f"Format: $format")
    try {
      // if value is null/empty string and nullable
      if (isNullValue(value) && isNullable)
        return true
      Utils.trimString(toReplace = ",", fromReplace = value).toInt
    }
    catch {
      case _: Exception =>
        valid = false
    }
    valid
  }

  /**
   *
   * @param value      literal value string to validate
   * @param isNullable true for nullable, false for not nullable
   * @return true if valid, false if invalid
   * @return
   */
  def isDouble(value: String, format: String = "", isNullable: Boolean): Boolean = {
    val valid = true
    logDebug(f"Format: $format")
    // if value is null/empty string and nullable
    try {
      if (isNullValue(value) && isNullable) {
        return true
      }
      value.toDouble
    }
    catch {
      case _: Exception =>
        return false
    }
    valid & value.toDouble.toString != "Infinity"
  }

  /**
   *
   * @param value      literal value string to validate
   * @param isNullable true for nullable, false for not nullable
   * @return true if valid, false if invalid
   * @return
   */
  def isBigInt(value: String, format: String = "", isNullable: Boolean): Boolean = {
    var valid = true
    var x = true
    logDebug(f"Format: $format")

    try {
      if (isNullValue(value) && isNullable)
        return true
      BigInteger.valueOf(value.toLong)
      x = value.toFloat.toString != "Infinity"
    }
    catch {
      case _: Exception =>
        valid = false
    }

    valid & x
  }

  /**
   *
   * @param value literal value string to validate
   * @return true if valid, false if invalid
   */
  def isNullValue(value: String): Boolean = {
    var output: Boolean = false
    try {

      if (value == null) {
        return true
      }
      val isValueNullString = value.replace("\n", "").equals("null")
      val isValueEmptyString = value.trim.isEmpty
      output = isValueNullString || isValueEmptyString
    }
    catch {
      case exception: Throwable =>
        logError(f"could not check nullability for value $value")
        logError(exception.getMessage)
        throw exception
    }
    output
  }

  /**
   *
   * @param value      literal value string to validate
   * @param isNullable true for nullable, false for not nullable
   * @return true if valid, false if invalid
   * @return
   */
  def isString(value: String, format: String = "", isNullable: Boolean): Boolean = {
    val valid = true
    logDebug(f"Format: $format")

    // if value is null/empty string and not nullable
    if (isNullValue(value) && !isNullable)
      return false
    valid
  }

  /**
   *
   * @param value      literal value string to validate
   * @param isNullable true for nullable, false for not nullable
   * @return true if valid, false if invalid
   * @return
   */
  def isTimeStamp(value: String, format: String, isNullable: Boolean): Boolean = {
    isDate(value, format, isNullable)
  }

  /**
   *
   * @param value      literal value string to validate
   * @param isNullable true for nullable, false for not nullable
   * @return true if valid, false if invalid
   * @return
   */
  def isDate(value: String, format: String, isNullable: Boolean): Boolean = {
    var valid = true

    try {
      if (isNullValue(value) && isNullable)
        return true
      DateTimeFormat.forPattern(format).parseDateTime(value)
    }
    catch {
      case _: Exception =>
        valid = false
    }

    //TODO Find better logic to handle this scenario
    valid //&& (value.length == format.length)
  }

  /**
   *
   * @param value      literal value string to validate
   * @param isNullable true for nullable, false for not nullable
   * @return true if valid, false if invalid
   * @return
   */
  def isDecimal(value: String, format: String, isNullable: Boolean): Boolean = {
    var valid = true
    try {
      if (isNullValue(value) && isNullable)
        return true

      val BigDecimalValue = BigDecimal(Utils.trimString(toReplace = ",", fromReplace = value))
      val precision = format.split(',')(0).toInt
      val scale = format.split(',')(1).toInt
      Decimal.apply(BigDecimalValue, precision, scale)
    }
    catch {
      case _: Exception =>
        valid = false
    }
    valid
  }
}
