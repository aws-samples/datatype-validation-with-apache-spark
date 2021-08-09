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

package com.awsproserve.common

import com.awsproserve.validation.DataType

object Constant {
  val INT = "INT"
  val STRING = "STRING"
  val BIGINT = "BIGINT"
  val DECIMAL = "DECIMAL"
  val DOUBLE = "DOUBLE"
  val DATE = "DATE"
  val TIMESTAMP = "TIMESTAMP"

  val acceptedDataTypes: List[String] = List(STRING,
    INT,
    BIGINT,
    DECIMAL,
    DOUBLE,
    DATE,
    TIMESTAMP)

  val requireFormatForDataTypes: List[String] = List(
    "DECIMAL",
    "DATE",
    "TIMESTAMP")

  val doubleQuoteChar = "\""

  val validateMethodMap: Map[String, (String, String, Boolean) => Boolean] = Map(
    INT -> DataType.isInteger,
    BIGINT -> DataType.isBigInt,
    DECIMAL -> DataType.isDecimal,
    DOUBLE -> DataType.isDouble,
    DATE -> DataType.isDate,
    TIMESTAMP -> DataType.isTimeStamp,
    STRING -> DataType.isString
  )
}
