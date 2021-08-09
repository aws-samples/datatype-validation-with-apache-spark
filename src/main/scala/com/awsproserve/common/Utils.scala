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

import scala.collection.mutable.ListBuffer

/**
 * Contains various com.awsproserve.common utility functions
 */
object Utils {
  /**
   * converts given list of case class object to Map
   *
   * @param caseClassList case class list
   * @return
   */
  def caseClassListToMap(caseClassList: List[AnyRef]): List[Map[String, Any]] = {
    val output: ListBuffer[Map[String, Any]] = ListBuffer[Map[String, Any]]()
    for (caseClass <- caseClassList) {
      output += caseClassToMap(caseClass).-("$outer")
    }
    output.toList
  }

  /**
   * converts given case class object to Map
   *
   * @param caseClass case class object
   * @return
   */
  def caseClassToMap(caseClass: AnyRef): Map[String, Any] =
    (Map[String, Any]() /: caseClass.getClass.getDeclaredFields) {
      (key, value) =>
        value.setAccessible(true)
        key + (value.getName -> value.get(caseClass))
    }

  /**
   * replaces toReplace string from given from Replace string
   *
   * @param toReplace   string to replace
   * @param fromReplace string to be replaced
   * @return
   */
  def trimString(toReplace: String, fromReplace: String): String = {
    if (fromReplace == null) {
      return null
    }
    fromReplace.trim.replaceAll(toReplace, "")
  }
}
