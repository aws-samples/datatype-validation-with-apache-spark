import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}
import org.scalatest.FunSuite
import com.awsproserve.validation.Delimited

class ValidationDelimitedTest extends FunSuite with Logging {

  val spark: SparkSession = SparkSession.builder.master("local[*]").enableHiveSupport().getOrCreate()

  def getJsonTestString: String = {
    """
      |{
      |  "schemaVersion": "0.0.1",
      |  "delimiter": ",",
      |  "fileHeader": true,
      |  "quoteChar": "\"",
      |  "escapeChar": "\\",
      |  "lineSeparator": "\n",
      |  "dataStructure": [
      |    {
      |      "fieldName": "int1",
      |      "dataType": "int",
      |      "nullable": true
      |    },
      |    {
      |      "fieldName": "int2",
      |      "dataType": "int",
      |      "nullable": false
      |    },
      |    {
      |      "fieldName": "string1",
      |      "dataType": "string",
      |      "nullable": true
      |    },
      |    {
      |      "fieldName": "string2",
      |      "dataType": "string",
      |      "nullable": false
      |    },
      |    {
      |      "fieldName": "decimal1",
      |      "dataType": "decimal",
      |      "dataFormat": "5,2",
      |      "nullable": true
      |    },
      |    {
      |      "fieldName": "decimal2",
      |      "dataType": "decimal",
      |      "dataFormat": "5,2",
      |      "nullable": false
      |    },
      |    {
      |      "fieldName": "date1",
      |      "dataType": "date",
      |      "nullable": true,
      |      "dataFormat": "yyyy-MM-dd"
      |
      |    },
      |    {
      |      "fieldName": "date2",
      |      "dataType": "date",
      |      "nullable": false,
      |      "dataFormat": "yyyy-MM-dd"
      |
      |    },
      |    {
      |      "fieldName": "timestamp1",
      |      "dataType": "timestamp",
      |      "nullable": true,
      |      "dataFormat": "yyyy/MM/dd'T'HH:mm:ss'Z'"
      |    },
      |    {
      |      "fieldName": "timestamp2",
      |      "dataType": "timestamp",
      |      "nullable": false,
      |      "dataFormat": "yyyy/MM/dd'T'HH:mm:ss'Z'"
      |    }
      |  ]
      |}
      |""".stripMargin
  }

  test("com.awsproserve.validation.ValidationDelimited.validate") {
    markup("validate: valid JSON metadata")
    val file = "src/test/resources/delimited_file.txt"
    val df = Delimited.validate(spark = spark, jsonMultilineString = getJsonTestString, file = file)
    df.printSchema()
//    df.show(100, truncate = false)
    df.filter(col("error_flag") === 0).show(100, truncate = false)
    df.filter(col("error_flag") === 1).show(100, truncate = false)
    df.filter(col("error_flag") === 2).show(100, truncate = false)
    df.filter(col("error_flag") === 3).show(100, truncate = false)
    df.coalesce(1)
      .withColumn("str_error_message", col("error_message").cast("STRING"))
      .drop(col("error_message"))
      .write.mode("overwrite").csv("src/test/resources/output/data")

    assert(df.count == 58)
    assert(df.filter(col("error_flag") === 0).count == 18)
    assert(df.filter(col("error_flag") === 1).count == 33)
    assert(df.filter(col("error_flag") === 2).count == 2)
    assert(df.filter(col("error_flag") === 3).count == 5)

    val actual: Long = df.filter(col("error_flag") === 1)
      .select(explode(col("error_message")).as("exp_error_message"))
      .filter(col("exp_error_message.field") === "int2").count
    assert(actual == 5)

    val actual1: String = df.filter(col("error_flag") === 1)
      .select(explode(col("error_message")).as("exp_error_message"))
      .filter(col("exp_error_message.field") === "int1")
      .select(col("exp_error_message.value"))
      .rdd.map(_ (0).toString)
      .collect
      .toList
      .sorted
      .mkString(",")

    assert(actual1 == "111.11,2021-01-01,some_text")
  }
}
