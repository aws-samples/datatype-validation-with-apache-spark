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

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class DelimitedText(delimiter: Char, quoteChar: Char, escapeChar: Char, lineSeparator: String, ignoreLeadingWhiteSpace: Boolean, ignoreTrailingWhiteSpace: Boolean, headerExtractionEnabled: Boolean) extends java.io.Serializable {
  // Setting static input buffer size
  // private val inputBufferSize: Int = 128
  // Default size is  1024*1024 characters

  lazy val parser: CsvParser = {
    val settings = new CsvParserSettings()

    settings.setIgnoreLeadingWhitespaces(ignoreLeadingWhiteSpace)
    settings.setIgnoreTrailingWhitespaces(ignoreTrailingWhiteSpace)
    settings.setReadInputOnSeparateThread(false)
    settings.setNullValue("")
    settings.getFormat.setLineSeparator(lineSeparator)
    settings.getFormat.setDelimiter(delimiter)
    settings.getFormat.setQuote(quoteChar)
    settings.getFormat.setQuoteEscape(escapeChar)
    settings.getFormat.setQuoteEscape(quoteChar)

    // TODO not sure how to set header, going for a workaround at the moment
    new CsvParser(settings)
  }

  def parse(spark: SparkSession, fileLocation: String): RDD[Array[String]] = {
    val context = spark.sparkContext
    val rdd: RDD[String] = readFileAsRDD(context = context, fileLocation = fileLocation)
    var finalRdd = rdd

    // If file headerExtractionEnabled is true then ignore lines that match file header
    if(headerExtractionEnabled){
      val header = rdd.first()
      finalRdd = rdd.mapPartitions{iterator => iterator.filter(_ != header)}
    }

    val output: RDD[Array[String]] = finalRdd
      .mapPartitions({
        iterator => {
          iterator.map(line => parser.parseLine(line))
        }
      })
    output
  }

  def readFileAsRDD(context: SparkContext, fileLocation: String): RDD[String] = {
    context.textFile(fileLocation)
  }
}