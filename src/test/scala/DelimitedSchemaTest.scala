/**
 * TODO code file header
 */

import org.apache.spark.internal.Logging
import org.scalatest.FunSuite
import com.awsproserve.parser.DelimitedSchema
import com.awsproserve.parser.DelimitedSchema.JSONSchemaDataStructure

class DelimitedSchemaTest extends FunSuite with Logging {
  def getJsonTestString: String = {
    """
      |{
      |  "schemaVersion": "0.0.1",
      |  "delimiter": ",",
      |  "fileHeader": true,
      |  "dataStructure": [
      |    {
      |      "fieldName": "column1",
      |      "dataType": "int",
      |      "nullable": false
      |    },
      |    {
      |      "fieldName": "column2",
      |      "dataType": "string",
      |      "nullable": true
      |    },
      |    {
      |      "fieldName": "column3",
      |      "dataType": "decimal",
      |      "dataFormat": "18,2",
      |      "nullable": false
      |    },
      |    {
      |      "fieldName": "column4",
      |      "dataType": "date",
      |      "nullable": false,
      |      "dataFormat": "yyyy/MM/dd"
      |
      |    },
      |    {
      |      "fieldName": "column5",
      |      "dataType": "timestamp",
      |      "nullable": true,
      |      "dataFormat": "yyyy/MM/dd HH:mm:ss"
      |    }
      |  ]
      |}
      |""".stripMargin
  }

  test("com.awsproserve.parser.SchemaParserDelimited.parseSchema"){
    markup("parseSchema: valid JSON metadata")
    val schemaParser = new DelimitedSchema(getJsonTestString)
    val actual = schemaParser.parseSchema
    val expected = List(
    JSONSchemaDataStructure(fieldName = "column1",dataType = "INT",nullable = false, dataFormat = None),
    JSONSchemaDataStructure(fieldName = "column2",dataType = "STRING",nullable = true, dataFormat = None),
    JSONSchemaDataStructure(fieldName = "column3",dataType = "DECIMAL",nullable = false, dataFormat = Some("18,2")),
    JSONSchemaDataStructure(fieldName = "column4",dataType = "DATE",nullable = false, dataFormat = Some("yyyy/MM/dd")),
    JSONSchemaDataStructure(fieldName = "column5",dataType = "TIMESTAMP",nullable = true, dataFormat = Some("yyyy/MM/dd HH:mm:ss")))
    assert(actual == expected)
  }

  test("com.awsproserve.parser.SchemaParserDelimited.getDelimiter") {
    markup("getDelimiter: valid JSON metadata")
    val schemaParser = new DelimitedSchema(getJsonTestString)
    val actual = schemaParser.getDelimiter
    val expected = ","
    assert(actual == expected)
  }

  test("com.awsproserve.parser.SchemaParserDelimited.getFileHeader") {
    markup("getFileHeader: valid JSON metadata")
    val schemaParser = new DelimitedSchema(getJsonTestString)
    val actual = schemaParser.getFileHeader
    val expected = true
    assert(actual == expected)
  }
}
