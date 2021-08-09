# Data Type Validation With Apache Spark

The delimited data type and structure validation framework validates input data against expected schema including number of columns, data types, nullability and assigns error flag and error message column to label specific error details. The purpose of this framework is to keep both the valid and invalid data together and have a mechanism to query specific erroneous data. The framework takes a schema details in a JSON format, input data path and returns a Spark DataFrame object that contains input data labelled with error flag and error message details.

The framework uses Spark native APIs such as RDD, DataFrame and in addition Univocity parser library that is used by Apache Spark and various other Apache open-source projects.

# Error Columns

The framework adds two columns that depicts data structure error details to the actual data set. Theese column are "error_flag" and "error_message".

| `Column` | `Type` | <div style="width:290px">`Description`</div> |
|---|---|---|
| error_flag | INT | indicates error flag as an integer value from 0 to 3.<br><br> <li> 0 = No Error <li> 1 = Data Type Error(s) <li> 2 = Num of Columns Less Than expected <li> 3 = Num of Columns More Than expected |
| error_message | Array[Map[String, String]] | contains error message with specific data components.<br><br> <li>If error_flag is 0 then it is an empty array. <li>If error_flag is 1 then it is an array of Map/dicts for all columns having data type validation errors as:<br><br>"field" -> "{Field name}"<br>"value" -> "{Field Value that caused error}"<br> "dataType" -> "{Field data type}"<br> "dataFormat" -> "{Field data format}"<br>"nullability" -> "{Field nullability}"<br><br>  <li> If error_flag is 2 or 3 it is an array of Map/dicts for all columns having number of columns not as expected as:<br><br>"record" -> "{Entire record as a single column}" <br>"expectedNumberOfColumns" -> "{Expected number of columns}"<br>"actualNumberOfColumns" -> "{Actual number of columns}"<br> |


# Schema Structure

The expected data structure is passed as a JSON string with specific details that describe the input data. The definition is as below:

`Element Name` |`Type`|<div style="width:290px">`Description`</div>|<div style="width:290px">`Mandatory Field/Default value`</div>|`Example`|
-------------|-------------|------------------------------------|---------|-------|
schemaVersion|String|Schema version|Mandatory|1.0.0|
delimiter    |String|Field delimiter as a single character|Mandatory|","
fileHeader   |String|Boolean Flag, true if data contains first line as header true or else false|Mandatory| true
quoteChar    |String|Field quote character|Optional, Default value double quote|"\\""
escapeChar   |String|Escape character |Optional, Default value back slash| "\\\\"
lineSeparator|String|Line separator|Optional, Default value new line|"\\n"
dataStructure|Array|Data structure of each column|Mandatory|{<br>"fieldName": "some_date_column",<br>"dataType": "date",<br>"nullable": false,<br>"dataFormat": "yyyy-MM-dd"<br>}
dataStructure.fieldName|String|Field name|Mandatory|sales_date
dataStructure.dataType|String|Field data type|Mandatory|Accepted Hive/Spark Data Types are:<br>STRING<br><li>INT<br><li>BIGINT<br><li>DOUBLE<br><li>DECIMAL<br><li>DATE<br><li>TIMESTAMP
dataStructure.nullable|Boolean|true if nullable, else false|Mandatory|false
dataStructure.dataFormat|String|Data type format. Required for DECIMAL, DATE and TIMESTAMP data types|Mandatory|<li>For DECIMAL Format = precision, scale i.e., 5,2<br><li>For DATE Format = [Java Date Format](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html) Format i.e., "yyyy-MM-dd"<br><li>For TIMESTAMP Format = [Java Date Time Format](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html) Format i.e., "yyyy-MM-dd HH:mm:ss", Zulu format: "yyyy-MM-dd'T'HH:mm:ss'Z'"
### Structure Template

```json
{
  "schemaVersion": "<schemaVersion value>",
  "delimiter": "<delimiter value>",
  "fileHeader": "<fileHeader value>",
  "quoteChar": "<quoteChar value>",
  "escapeChar": "<escapeChar value>",
  "lineSeparator": "<lineSeparator value>",
  "dataStructure": [
    {
      "fieldName": "<field name>",
      "dataType": "<field data type>",
      "nullable": "<nullable boolean value>",
      "dataFormat": "<respective data type format>"
    },
    ...
    ...
    ...
    ...
  ]
}
```

### Example
```json
{
  "schemaVersion": "0.0.1",
  "delimiter": ",",
  "fileHeader": true,
  "quoteChar": "\"",
  "escapeChar": "\\",
  "lineSeparator": "\n",
  "dataStructure": [
    {
      "fieldName": "int1",
      "dataType": "int",
      "nullable": true
    },
    {
      "fieldName": "int2",
      "dataType": "int",
      "nullable": false
    },
    {
      "fieldName": "string1",
      "dataType": "string",
      "nullable": true
    },
    {
      "fieldName": "string2",
      "dataType": "string",
      "nullable": false
    },
    {
      "fieldName": "decimal1",
      "dataType": "decimal",
      "dataFormat": "38,0",
      "nullable": true
    },
    {
      "fieldName": "decimal2",
      "dataType": "decimal",
      "dataFormat": "5,2",
      "nullable": false
    },
    {
      "fieldName": "date1",
      "dataType": "date",
      "nullable": true,
      "dataFormat": "yyyy-MM-dd"

    },
    {
      "fieldName": "date2",
      "dataType": "date",
      "nullable": false,
      "dataFormat": "yyyy-MM-dd"

    },
    {
      "fieldName": "timestamp1",
      "dataType": "timestamp",
      "nullable": true,
      "dataFormat": "yyyy/MM/dd'T'HH:mm:ss'Z'"
    },
    {
      "fieldName": "timestamp2",
      "dataType": "timestamp",
      "nullable": false,
      "dataFormat": "yyyy/MM/dd'T'HH:mm:ss'Z'"
    }
  ]
}
```
# Clone Repository

```bash
git clone {GIT Repo URL}
cd datatype-validation-with-apache-spark
```

# Open JDK 8 setup

```bash
brew install openjdk@8
export JAVA_HOME=/usr/local/opt/openjdk@8
```
> For Windows and Linux corresponding utilities can be used to download and install Open JDK

# Install sbt

```bash
brew install sbt
```
# Run Tests

```bash
sbt test
```

# Build Package
SBT pack plugin compiles the source code and creates a ```pack/lib``` directory under ```datatype-validation-with-apache-spark/target``` that contains individual JAR files required
```bash
sbt pack
```
```
spark-datatype-com-awsproserve-validation_2.11-1.0.jar
play-functional_2.11-2.7.4.jar
play-json_2.11-2.7.4.jar
```

Above is the list of JAR files required to be added to Apache Spark(AWS Glue, Amazon EMR or any Spark system) setup as:
```
--jars {path}/play-functional_2.11-2.7.4.jar,{path}/play-json_2.11-2.7.4.jar,{path}/spark-datatype-com-awsproserve-validation_2.11-1.0.jar
```

# Data Analysis

The document available at [Data Analysis](docs/how_to_analyze.asciidoc) contains code example and AWS Athena queries to analyze data using the error information derived by the validation framework.

# Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

# License

This library is licensed under the MIT-0 License. See the LICENSE file.