package pub.ayada.scala.sparkUtils.etl

import pub.ayada.scala.utils.DateTimeUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

case class Jdbc2DFProps(taskType: String = "Jdbc2DF",
                        id: String,
                        propsFile: String,
                        schema: String,
                        table: String,
                        fetchSize: String = "10000",
                        isolationLevel: String = "READ_COMMIT",
                        loadCount: Boolean = false,
                        printSchema: Boolean = true,
                        sql: String,
                        partitionKey: String,
                        noOfPartitions: String,
                        lowerBound: String,
                        upperBound: String,
                        connParms: scala.collection.mutable.Map[String, String])

class Jdbc2DF(sqlContext: org.apache.spark.sql.SQLContext,
              hiveContext: org.apache.spark.sql.hive.HiveContext,
              props: pub.ayada.scala.sparkUtils.etl.Jdbc2DFProps,
              dfs: scala.collection.mutable.Map[String, (org.apache.spark.sql.DataFrame, Boolean)]) {

    val dfOptions = props.connParms

    var lowerBound = props.lowerBound
    var upperBound = props.upperBound
    val partKey = props.partitionKey.toUpperCase
    val noOfPartitions = props.noOfPartitions
    val jobID = "Jdbc2DF-" + props.id

    def execute() = {
        import sqlContext.sql
        import sqlContext.implicits._
        if ("" != partKey && "NONE" != partKey &&
            "" == lowerBound &&
            "" == upperBound) {
            val boundSql: StringBuilder = new StringBuilder("(select")
            boundSql.append("  min(").append(partKey).append(") as minKey ")
                .append(", max(").append(partKey).append(") as maxKey ")
                .append(" from ").append("(" + props.sql + ")) tmp")
            println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobID) + "Retrieving  the lower and upper bounds using the below SQL")
            println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobID) + boundSql.toString)
            var dframeBounds = sqlContext.read.format("jdbc")
                .options(dfOptions)
                .option("dbtable", boundSql.toString)
                .load()
            lowerBound = dframeBounds.map(t => t(0)).collect()(0).toString
            upperBound = dframeBounds.map(t => t(1)).collect()(0).toString
            sqlContext.emptyDataFrame
            sqlContext.clearCache
        }

        dfOptions += ("dbtable" -> { "(" + props.sql + ") tmp" })
        dfOptions += ("fetchsize" -> props.fetchSize)
        dfOptions += ("isolationLevel" -> props.isolationLevel)
        if ("" != noOfPartitions) {
            dfOptions += ("numPartitions" -> noOfPartitions)
        }
        if ("" != lowerBound && "" != upperBound) {
            println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobID) + "Forcing lowerBound:" + lowerBound + " and upperBound: " + upperBound)
            dfOptions += ("lowerBound" -> lowerBound)
            dfOptions += ("upperBound" -> upperBound)
        }

        import hiveContext.sql
        import hiveContext.implicits._
        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobID) + "All Props:" + pub.ayada.scala.utils.ObjectUtils.prettyPrint(props))
        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobID) + "DataFrame options: " + pub.ayada.scala.utils.ObjectUtils.prettyPrint(dfOptions))

        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobID) + "Building DataFrame")
        val df = hiveContext.read.format("jdbc").options(dfOptions).load()
        if (props.printSchema) {
            println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobID) + "DF schema of : jdbc " + " :" + df.schema.treeString)
        }
        val df2 = df.persist(StorageLevel.MEMORY_AND_DISK)
        dfs.put(props.id, (df2, props.loadCount))
    }
}

object Jdbc2DF {
    /**
     * extracts the JDBC jobinfo from the input xml node object. If a property is
     * defined in both propsFile and in the xml, the value in the xml will take precedence
     * <br>List of properties, <code>
     * <br>jdbc2hive \ "jdbc" \ "@propsFile" &nbsp;&nbsp; (default:null)
     * <br>jdbc2hive \ "jdbc" \ "@schema" &nbsp;&nbsp; (default:null)
     * <br>jdbc2hive \ "jdbc" \ "@table" &nbsp;&nbsp; (default:null)
     * <br>jdbc2hive \ "jdbc" \ "@fetchSize" &nbsp;&nbsp; (default: "100000")
     * <br>jdbc2hive \ "jdbc" \ "@isolationLevel" &nbsp;&nbsp; (default:"READ_COMMIT")
     * <br>jdbc2hive \ "jdbc" \ "@loadCount" &nbsp;&nbsp; (default: "false")
     * <br>jdbc2hive \ "jdbc" \ "@printSchema" &nbsp;&nbsp; (default: "false")
     * <br>jdbc2hive \ "jdbc" \ "sql" &nbsp;&nbsp; (default:null)
     * <br>
     * <br>jdbc2hive \ "hive" \ "@loadType" &nbsp;&nbsp; (default: append)
     * <br>jdbc2hive \ "hive" \ "@schema" &nbsp;&nbsp; (default: "default")
     * <br>jdbc2hive \ "hive" \ "@table" &nbsp;&nbsp; (default:null)
     * <br>jdbc2hive \ "hive" \ "@partitionKey" &nbsp;&nbsp; (default:null)
     * <br>jdbc2hive \ "hive" \ "@noOfPartitions" &nbsp;&nbsp; (default:null)
     * <br>jdbc2hive \ "hive" \ "@lowerBound" &nbsp;&nbsp; (default:null)
     * <br>jdbc2hive \ "hive" \ "@upperBound" &nbsp;&nbsp; (default:null)
     * <br>jdbc2hive \ "hive" \ "@preLoadCount" &nbsp;&nbsp; (default: "false")
     * <br>jdbc2hive \ "hive" \ "@postLoadCount" &nbsp;&nbsp; (default:null)
     * <code>
     */
    def getJdbc2DFProps(jobxmlNode: scala.xml.Node, sc: org.apache.spark.SparkContext): List[Jdbc2DFProps] = {
        val props = for (jdbc <- (jobxmlNode \ "jdbc")) yield {
            getJobProps(jdbc, sc)
        }
        props.toList
    }

    private def getJobProps(jobxmlNode: scala.xml.Node, sc: org.apache.spark.SparkContext): Jdbc2DFProps = {
        var JobProps: java.util.Properties = new java.util.Properties
        val JDBCPropsFile = (jobxmlNode \ "@propsFile").text.trim
        val JDBCId = (jobxmlNode \ "@id").text.trim
        val JDBCSchema = (jobxmlNode \ "@schema").text.trim
        val JDBCTable = (jobxmlNode \ "@table").text.trim
        val JDBCFetchSize = (jobxmlNode \ "@fetchSize").text.trim
        val JDBCIsolationLevel = (jobxmlNode \ "@isolationLevel").text.trim
        val JDBCLoadCount = (jobxmlNode \ "@loadCount").text.trim.toLowerCase
        val JDBCPrintSchema = (jobxmlNode \ "@printSchema").text.trim.toLowerCase
        val JDBCRegisterTempTable = (jobxmlNode \ "@registerTempTable").text.trim.toLowerCase
        val HivePartitionKey = (jobxmlNode \ "@partitionKey").text.trim
        val HiveNoOfPartitions = (jobxmlNode \ "@noOfPartitions").text.trim
        val HiveLowerBound = (jobxmlNode \ "@lowerBound").text.trim
        val HiveUpperBound = (jobxmlNode \ "@upperBound").text.trim

        val JDBCSql = (jobxmlNode \ "sql").text.trim
        if ("" != JDBCPropsFile) {
            JobProps.putAll(pub.ayada.scala.utils.hdfs.HdfsFileIO.getProps(JDBCPropsFile))
        }

        val props = new Jdbc2DFProps(id = JDBCId,
            propsFile = JDBCPropsFile,
            schema = getOrElse(JobProps, "schema", JDBCSchema),
            table = getOrElse(JobProps, "table", JDBCTable),
            fetchSize = getOrElse(JobProps, "fetchSize", JDBCFetchSize),
            isolationLevel = getOrElse(JobProps, "isolationLevel", JDBCIsolationLevel),
            loadCount = ("true" == getOrElse(JobProps, "loadCount", JDBCLoadCount)),
            printSchema = ("true" == getOrElse(JobProps, "printSchema", JDBCPrintSchema)),
            sql = {
                if ("" != JDBCSql)
                    JDBCSql
                else {
                    println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Jdbc2DF") + "Custom SQL not provided. Using the input schema and table to build the sql.\n")
                    if ("" != JDBCSchema) {
                        "select * from " + JDBCSchema + "." + JDBCTable
                    } else {
                        "select * from " + JDBCTable
                    }
                }
            },
            partitionKey = getOrElse(JobProps, "partitionKey", HivePartitionKey),
            noOfPartitions = getOrElse(JobProps, "noOfPartitions", HiveNoOfPartitions),
            lowerBound = getOrElse(JobProps, "lowerBound", HiveLowerBound),
            upperBound = getOrElse(JobProps, "upperBound", HiveUpperBound),
            connParms = scala.collection.mutable.Map("url" -> getOrFail(JobProps, "JDBCUrl"),
                "driver" -> getOrFail(JobProps, "JDBCDriver"),
                "user" -> getOrFail(JobProps, "JDBCUser"),
                "password" -> getOrElse(JobProps, "JDBCPassword", getPswdFromAlias(JobProps, sc)))
        )

        props
    }

    def getOrElse(props: java.util.Properties, key: String, default: String): String = {

        if (props.containsKey(key)) {
            return props.getProperty(key)
        }
        default
    }

    def getOrFail(props: java.util.Properties, key: String): String = {

        if (!props.containsKey(key))
            throw new Exception("JDBC connection property '" + key + "' Not received.")
        props.getProperty(key)
    }

    def getPswdFromAlias(props: java.util.Properties, sparkContext: org.apache.spark.SparkContext): String = {

        if (!props.containsKey("JDBCPasswordAlias"))
            throw new Exception("JDBC connection property [JDBCPasswordAlias | JDBCPassword ] Not received.")
        if (!sparkContext.getConf.contains(("spark.hadoop.hadoop.security.credential.provider.path")))
            throw new Exception("Please pass keystore file path in the command line (--conf spark.hadoop.hadoop.security.credential.provider.path=jceks://hdfs/<path to file>")

        try {
            new String(sparkContext.hadoopConfiguration.getPassword(props.getProperty("JDBCPasswordAlias")))
        } catch {
            case t: Throwable => throw new Exception("Failed to retrive password for alias: '" + props.getProperty("JDBCPasswordAlias") + "' from the kestore file saved in the keystore:hadoop.security.credential.provider.path", t)
        }
    }

}
