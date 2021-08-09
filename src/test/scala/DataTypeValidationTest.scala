import org.apache.spark.internal.Logging
import org.scalatest.FunSuite
import com.awsproserve.validation.DataType

class DataTypeValidationTest extends FunSuite with Logging {
  /**
   * isInteger
   */
  test("com.awsproserve.validation.DataTypeValidationTest.isInteger") {
    markup("isInteger: valid integer as not null")
    var value = "1"
    var isNullable = false
    var validation = DataType.isInteger(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)

    markup("isInteger: invalid integer as not null")
    value = "a"
    isNullable = false
    validation = DataType.isInteger(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isInteger: valid integer as nullable")
    value = "1"
    isNullable = true
    validation = DataType.isInteger(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)

    markup("isInteger: invalid integer as nullable")
    value = "a"
    isNullable = true
    validation = DataType.isInteger(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isInteger: NULL value as nullable")
    value = ""
    isNullable = true
    validation = DataType.isInteger(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)

    markup("isInteger: NULL value as NOT nullable")
    value = ""
    isNullable = false
    validation = DataType.isInteger(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isInteger: invalid out of bound integer as nullable")
    value = "99999999999999999999999999999999999999999999999999"
    isNullable = true
    validation = DataType.isInteger(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)
  }

  /**
   * isNullValue
   */
  test("com.awsproserve.validation.DataTypeValidationTest.isNullValue") {
    markup("isNullValue: empty string")
    var value = ""
    var validation = DataType.isNullValue(value = value)
    logInfo(f"Value = '$value' Validation: $validation")
    assert(validation)

    markup("isNullValue: empty string and isNullable false")
    value = ""
    validation = DataType.isNullValue(value = value)
    logInfo(f"Value = '$value' Validation: $validation")
    assert(validation)

    markup("isNullValue: null string")
    value = "null"
    validation = DataType.isNullValue(value = value)
    logInfo(f"Value = '$value' Validation: $validation")
    assert(validation)

    markup("isNullValue: null value")
    value = null
    validation = DataType.isNullValue(value = value)
    logInfo(f"Value = '$value' Validation: $validation")
    assert(validation)

    markup("isNullValue: non empty/null string")
    value = "test"
    validation = DataType.isNullValue(value = value)
    logInfo(f"Value = '$value' Validation: $validation")
    assert(!validation)
  }

  /**
   * isString
   */
  test("com.awsproserve.validation.DataTypeValidationTest.isString") {
    markup("isString: non empty string")
    var value = "test"
    var isNullable = true
    var validation = DataType.isString(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)

    markup("isString: empty string as not null")
    value = ""
    isNullable = false
    validation = DataType.isString(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isString: empty string as null")
    value = ""
    isNullable = true
    validation = DataType.isString(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)
  }

  /**
   * isDouble
   */
  test("com.awsproserve.validation.DataTypeValidationTest.isDouble") {
    markup("isDouble: valid double as not null")
    var value = "1000.987"
    var isNullable = false
    var validation = DataType.isDouble(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)

    markup("isDouble: invalid double as not null")
    value = "a"
    isNullable = false
    validation = DataType.isDouble(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isDouble: valid double as nullable")
    value = "99999.999999999"
    isNullable = true
    validation = DataType.isDouble(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)

    markup("isDouble: invalid double as nullable")
    value = "a"
    isNullable = true
    validation = DataType.isDouble(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isDouble: NULL value as nullable")
    value = ""
    isNullable = true
    validation = DataType.isDouble(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)

    markup("isDouble: NULL value as NOT nullable")
    value = ""
    isNullable = false
    validation = DataType.isDouble(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)
  }

  /**
   * isBigInt
   */
  test("com.awsproserve.validation.DataTypeValidationTest.isBigInt") {
    markup("isBigInt: valid bigint as not null")
    var value = "1000"
    var isNullable = false
    var validation = DataType.isBigInt(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)

    markup("isBigInt: invalid bigint as not null")
    value = "a"
    isNullable = false
    validation = DataType.isBigInt(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isBigInt: valid bigint as nullable")
    value = "999999999999999999" // 19 digits
    isNullable = true
    validation = DataType.isBigInt(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)

    markup("isBigInt: invalid out of bound bigint as nullable")
    value = "99999999999999999999999999999999999999999999999999"
    isNullable = true
    validation = DataType.isBigInt(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isBigInt: invalid bigint as nullable")
    value = "a"
    isNullable = true
    validation = DataType.isBigInt(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isBigInt: NULL value as nullable")
    value = ""
    isNullable = true
    validation = DataType.isBigInt(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)

    markup("isBigInt: NULL value as NOT nullable")
    value = ""
    isNullable = false
    validation = DataType.isBigInt(value = value, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)
  }

  /**
   * isDate
   */
  test("com.awsproserve.validation.DataTypeValidationTest.isDate") {
    markup("isDate: valid date as not null with valid format: 'yyyy-MM-dd'")
    var value = "2021-06-29"
    var isNullable = false
    var format = "yyyy-MM-dd"
    var validation = DataType.isDate(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)

    markup("isDate: NULL date as not null with valid format: 'yyyy-MM-dd'")
    value = ""
    isNullable = false
    format = "yyyy-MM-dd"
    validation = DataType.isDate(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isDate: NULL date as null with valid format: 'yyyy-MM-dd'")
    value = ""
    isNullable = true
    format = "yyyy-MM-dd"
    validation = DataType.isDate(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)

    markup("isDate: invalid date as not null with valid format: 'yyyy-MM-dd'")
    value = "2021-13-29"
    isNullable = false
    format = "yyyy-MM-dd"
    validation = DataType.isDate(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isDate: valid date as not null with invalid format: 'yyyy-MM-XX'")
    value = "2021-10-29"
    isNullable = false
    format = "yyyy-MM-XX"
    validation = DataType.isDate(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isDate: valid date as not null with no format")
    value = "2021-10-29"
    isNullable = false
    format = ""
    validation = DataType.isDate(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

  }

  /**
   * isTimeStamp
   */
  test("com.awsproserve.validation.DataTypeValidationTest.isTimeStamp") {
    markup("isTimeStamp: valid timestamp as not null with valid format: 'MM/dd/yyyy HH:mm:ss'")
    var value = "06/29/2021 01:02:03"
    var isNullable = false
    var format = "MM/dd/yyyy HH:mm:ss"
    var validation = DataType.isTimeStamp(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)

    markup("isTimeStamp: NULL timestamp as not null with valid format: 'MM/dd/yyyy HH:mm:ss'")
    value = ""
    isNullable = false
    format = "MM/dd/yyyy HH:mm:ss"
    validation = DataType.isTimeStamp(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isTimeStamp: NULL timestamp as null with valid format: 'MM/dd/yyyy HH:mm:ss'")
    value = ""
    isNullable = true
    format = "MM/dd/yyyy HH:mm:ss"
    validation = DataType.isTimeStamp(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)

    markup("isTimeStamp: invalid timestamp as not null with valid format: 'MM/dd/yyyy HH:mm:ss'")
    value = "06/55/2021 01:02:03"
    isNullable = false
    format = "MM/dd/yyyy HH:mm:ss"
    validation = DataType.isTimeStamp(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isTimeStamp: valid timestamp as not null with invalid format: 'MM/dd/yyyy XX:mm:ss'")
    value = "06/29/2021 01:02:03"
    isNullable = false
    format = "MM/dd/yyyy XX:mm:ss"
    validation = DataType.isTimeStamp(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isTimeStamp: valid timestamp as not null with no format")
    value = "06/29/2021 01:02:03"
    isNullable = false
    format = ""
    validation = DataType.isTimeStamp(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)
  }
  /**
   * isDecimal
   */
  test("com.awsproserve.validation.DataTypeValidationTest.isDecimal") {
    markup("isDecimal: valid decimal as not null with valid format: '5,2'")
    var value = "123.45"
    var isNullable = false
    var format = "5,2"
    var validation = DataType.isDecimal(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)

    markup("isDecimal: valid integer as not null with valid format: '5,2'")
    value = "12345"
    isNullable = false
    format = "5,2"
    validation = DataType.isDecimal(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isDecimal: valid decimal as not null with no format")
    value = "123.456789"
    isNullable = false
    format = ""
    validation = DataType.isDecimal(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isDecimal: invalid decimal as not null with valid format: '5,2'")
    value = "1234.56"
    isNullable = false
    format = "5,2"
    validation = DataType.isDecimal(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isDecimal: invalid decimal as null with valid format: '5,2'")
    value = "1234.56"
    isNullable = true
    format = "5,2"
    validation = DataType.isDecimal(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isDecimal: invalid decimal as nullable")
    value = "a"
    isNullable = true
    validation = DataType.isDecimal(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)

    markup("isDecimal: NULL value as nullable")
    value = ""
    isNullable = true
    validation = DataType.isDecimal(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(validation)

    markup("isDecimal: NULL value as NOT nullable")
    value = ""
    isNullable = false
    validation = DataType.isDecimal(value = value, format = format, isNullable = isNullable)
    logInfo(f"Value = '$value' Nullable: '$isNullable' Validation: $validation")
    assert(!validation)
  }

}