package pub.ayada.scala.sparkUtils.etl

/*
http://www.infoobjects.com/spark-sql-schemardd-programmatically-specifying-schema/
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext._
val person = sc.textFile("hdfs://localhost:9000/user/hduser/person")
import org.apache.spark.sql._
 val schema = StructType(Array(StructField("firstName",StringType,true),StructField("lastName",StringType,true),StructField("age",IntegerType,true)))
val rowRDD = person.map(_.split(",")).map(p =&gt; org.apache.spark.sql.Row(p(0),p(1),p(2).toInt))
val personSchemaRDD = sqlContext.applySchema(rowRDD, schema)
personSchemaRDD.registerTempTable("person")
sql("select * from person").foreach(println)

*/
import pub.ayada.scala.utils.DateTimeUtils
import org.apache.spark.sql.types.{ StructType, StructField }
import org.apache.spark.sql.types._
import java.time.LocalDateTime
import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

case class Csv2DFProps(taskType: String = "Csv2DF",
                       id: String,
                       propsFile: String,
                       path: String,
                       header: Boolean = true,
                       delimiter: String = ",",
                       quote: String = "\"",
                       escape: String = "\\",
                       mode: String = "PERMISSIVE",
                       charset: String = "UTF-8",
                       commentChar: String = null.asInstanceOf[String],
                       dateFormat: String = "M/d/yyyy H:m:s a",
                       nullValue: String,
                       printSchema: Boolean = true,
                       loadCount: Boolean = false,
                       csvColumns: StructType)

class Csv2DF(hiveContext: org.apache.spark.sql.hive.HiveContext,
             props: Csv2DFProps,
             dfs: scala.collection.mutable.Map[String, (org.apache.spark.sql.DataFrame, Boolean)]) {

    private val jobID = "Csv2DF-" + props.id
    //private val hiveContext = hiveCtx
    import hiveContext.sql
    import hiveContext.implicits._
    import org.apache.spark.sql._

    val dfOptions: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String](
        "header" -> props.header.toString,
        "delimiter" -> props.delimiter,
        "quote" -> props.quote,
        "escape" -> props.escape,
        "mode" -> props.mode,
        "charset" -> props.charset,
        "comment" -> { if ("" == props.commentChar) null else props.commentChar.charAt(0).toString },
        "dateFormat" -> props.dateFormat,
        "nullValue" -> props.nullValue)

    def execute() = {
        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobID) + "All Props:" + pub.ayada.scala.utils.ObjectUtils.prettyPrint(props))
        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobID) + "DataFrame options: " + pub.ayada.scala.utils.ObjectUtils.prettyPrint(dfOptions))
        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobID) + "Schema: " + pub.ayada.scala.utils.ObjectUtils.prettyPrint(props.csvColumns.simpleString))

        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobID) + "HiveContext: " + hiveContext.toString)

        val dfr = if (props.csvColumns.isEmpty) {
            dfOptions += ("inferSchema" -> "true")
            hiveContext.read
                .format("com.databricks.spark.csv")
                .options(dfOptions)
        } else {
            hiveContext.read
                .format("com.databricks.spark.csv")
                .options(dfOptions)
                .schema(props.csvColumns)
        }

        val DF_CSV = dfr.load(props.path)

        if (props.printSchema) {
            println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobID) + "DF schema of : " + props.id + "\n" + DF_CSV.schema.treeString)
        }

        val df2 = DF_CSV.persist(StorageLevel.MEMORY_AND_DISK)
        dfs.put(props.id, (df2, props.loadCount))
    }
}

object Csv2DF {
    /**
     * extracts the CSV jobinfo from the input xml node object. If a property is
     * defined in both propsFile and in the xml, the value in the xml will take precedence
     * <br><br>List of properties, <code>
     * <br>csv2hive \ "csv" \ "@propsFile" &nbsp;&nbsp; (default:null)
     * <br>csv2hive \ "csv" \ "@path" &nbsp;&nbsp; (default:null)
     * <br>csv2hive \ "csv" \ "@header" &nbsp;&nbsp; (default:false)
     * <br>csv2hive \ "csv" \ "@delimiter" &nbsp;&nbsp; (default:",")
     * <br>csv2hive \ "csv" \ "@quote" &nbsp;&nbsp; (default:'"')
     * <br>csv2hive \ "csv" \ "@escape" &nbsp;&nbsp; (default:"\")
     * <br>csv2hive \ "csv" \ "@mode" &nbsp;&nbsp; (default:PERMISSIVE)
     * <br>csv2hive \ "csv" \ "@charset" &nbsp;&nbsp; (default:UTF-8)
     * <br>csv2hive \ "csv" \ "@commentChar" &nbsp;&nbsp; (default:null)
     * Default is "#". Disable comments by setting this to null.
     * <br>csv2hive \ "csv" \ "@dateFormat" &nbsp;&nbsp; (default: null)
     * supports formats defined in java.text.SimpleDateFormat. if null,
     * java.sql.Timestamp.valueOf() and java.sql.Date.valueOf() will be used to
     * convert the string
     * <br>csv2hive \ "csv" \ "@nullValue" &nbsp;&nbsp; (default:null)
     * <br>csv2hive \ "csv" \ "@loadCount" &nbsp;&nbsp; (default:null)
     * <br>csv2hive \ "csv" \ "filterSql" &nbsp;&nbsp; (default:null) If
     * not mentioned, all the valid rows will be processed.
     * <br>csv2hive \ "csv" \ "schema" &nbsp;&nbsp; (default:null) If
     * not provided, schema will be inferred.
     * <br>csv2hive \ "csv" \ "schema" \  "@printSchema" &nbsp;&nbsp; (default:null)
     * <br>
     * <br>csv2hive \ "hive" \ "@loadType"
     * <br>csv2hive \ "hive" \ "@schema"
     * <br>csv2hive \ "hive" \ "@table"
     * <br>csv2hive \ "hive" \ "@preLoadCount" &nbsp;&nbsp; (default:false)
     * <br>csv2hive \ "hive" \ "@postLoadCount" &nbsp;&nbsp; (default:false)
     * <code>
     */

    def getCsv2DFProps(jobxmlNode: scala.xml.Node): List[Csv2DFProps] = {
        val props = for (csv <- (jobxmlNode \ "csv")) yield {
            getJobProps(csv)
        }
        props.toList
    }

    private def getJobProps(jobxmlNode: scala.xml.Node): Csv2DFProps = {
        var JobProps: java.util.Properties = new java.util.Properties()
        val CSVPropsFile = (jobxmlNode \ "@propsFile").text.trim
        val CSVPath = (jobxmlNode \ "@path").text.trim
        val CSVHeader = (jobxmlNode \ "@header").text.trim
        val CSVDelimiter = (jobxmlNode \ "@delimiter").text.trim
        val CSVQuote = (jobxmlNode \ "@quote").text.trim
        val CSVEscape = (jobxmlNode \ "@escape").text.trim
        val CSVMode = (jobxmlNode \ "@mode").text.trim.toUpperCase
        val CSVCharset = (jobxmlNode \ "@charset").text.trim
        val CSVCommentChar = (jobxmlNode \ "@commentChar").text.trim
        val CSVDateFormat = (jobxmlNode \ "@dateFormat").text.trim
        val CSVNullValue = (jobxmlNode \ "@nullValue").text.trim
        val CSVLoadCount = (jobxmlNode \ "@loadCount").text.trim.toLowerCase
        val CSVPrintSchema = (jobxmlNode \ "schema" \ "@printSchema").text.trim.toLowerCase

        val CSVColumns = StructType(for (col <- jobxmlNode \ "schema" \ "column") yield {
            StructField((col \ "@name").text.trim,
                pub.ayada.scala.sparkUtils.cmn.UtilFuncs.literal2SparkDataType((col \ "@dataType").text.trim),
                "false" != (col \ "@nullable").text.trim.toLowerCase
            )
        })
        if ("" != CSVPropsFile) {
            JobProps.putAll(pub.ayada.scala.utils.hdfs.HdfsFileIO.getProps(CSVPropsFile))
        }
        val csv2DFProps: Csv2DFProps = new Csv2DFProps(
            id = (jobxmlNode \ "@id").text.trim,
            propsFile = CSVPropsFile,
            path = getOrElse(JobProps, "CSVPropsFile", CSVPath),
            delimiter = getOrElse(JobProps, "CSVDelimiter", if ("" != CSVDelimiter) CSVDelimiter else ","),
            quote = getOrElse(JobProps, "CSVQuote", if ("" != CSVQuote) CSVQuote else "\""),
            escape = getOrElse(JobProps, "CSVEscape", if ("" != CSVEscape) CSVEscape else "\\"),
            mode = getOrElse(JobProps, "CSVMode", if ("" != CSVMode) CSVMode else "PERMISSIVE"),
            charset = getOrElse(JobProps, "CSVCharset", if ("" != CSVCharset) CSVCharset else "UTF-8"),
            commentChar = getOrElse(JobProps, "CSVCommentChar", if ("" != CSVCommentChar) String.valueOf(CSVCommentChar.charAt(0)) else ""),
            dateFormat = getOrElse(JobProps, "CSVDateFormat", if ("" != CSVDateFormat) CSVDateFormat else ""),
            nullValue = getOrElse(JobProps, "CSVNullValue", if ("" != CSVNullValue) CSVNullValue else ""),
            header = ("true" == getOrElse(JobProps, "CSVHeader", CSVHeader)),
            printSchema = ("true" == getOrElse(JobProps, "CSVPrintSchema", CSVPrintSchema)),
            loadCount = ("true" == getOrElse(JobProps, "CSVLoadCount", CSVLoadCount)),
            csvColumns = CSVColumns)

        csv2DFProps
    }
    def getOrElse(props: java.util.Properties, key: String, default: String): String = {

        if (props.containsKey(key)) {
            return props.getProperty(key)
        }
        default
    }
}
