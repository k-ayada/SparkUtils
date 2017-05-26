package pub.ayada.scala.sparkUtils

class Jdbc2Hive(sc: org.apache.spark.SparkContext, props: java.util.Properties, status: scala.collection.mutable.HashMap[String, Int]) extends Runnable {

    private val jobID = props.getProperty("jobId")
    private val dtFmt = new java.text.SimpleDateFormat("yyyyMMdd hh:mm:ss ")
    private def sysTime: String = {
        val sb: StringBuilder = new StringBuilder()
        sb.append("[")
            .append(dtFmt.format(new java.util.Date()))
            .append("][")
            .append(jobID)
            .append("] ")

        sb.toString
    }

    var mode = {
        if ("overwrite" == props.getProperty("HiveLoadType").toLowerCase)
            org.apache.spark.sql.SaveMode.Overwrite
        else org.apache.spark.sql.SaveMode.Append
    }

    val password: String = {
        if (props.containsKey("JDBCPassword"))
            props.getProperty("JDBCPassword")
        else if (props.containsKey("JDBCPasswordAlias"))
            new String(sc.hadoopConfiguration.getPassword(props.getProperty("JDBCPasswordAlias")))
        else {
            println(sysTime + "Properties for job: " + props.getProperty("jobId") + "\n" + props + "\n\n")
            throw new Exception("Need eithere JDBC password or alias saved in the keystore:hadoop.security.credential.provider.path")
        }
    }
    val dfOptions: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String](
        "url" -> props.getProperty("JDBCUrl"),
        "driver" -> props.getProperty("JDBCDriver"),
        "user" -> props.getProperty("JDBCUser"),
        "password" -> password)

    var lowerBound = if (props.containsKey("HiveLowerBound")) props.getProperty("HiveLowerBound") else ""
    var upperBound = if (props.containsKey("HiveUpperBound")) props.getProperty("HiveUpperBound") else ""
    val partKey = if (props.containsKey("HivePartitionKey")) props.getProperty("HivePartitionKey") else ""
    val noOfPartitions = if (props.containsKey("HiveNoOfPartitions")) props.getProperty("HiveNoOfPartitions") else ""
    val hiveSchema = if (props.containsKey("HiveSchema")) props.getProperty("HiveSchema").toUpperCase else "DEFAULT"
    val hiveTable = if (props.containsKey("HiveTable")) props.getProperty("HiveTable").toUpperCase else {
        throw new Exception("Hive Tblae name not found in the input configuration xml")
    }

    val hiveSchemaTable = hiveSchema + "." + hiveTable

    override def run() {
        try {
            val sqlContext = new org.apache.spark.sql.SQLContext(sc)
            import sqlContext.implicits._
            if ("" != partKey && "NONE" != partKey.toUpperCase() &&
                "" == lowerBound &&
                "" == upperBound) {
                val boundSql: StringBuilder = new StringBuilder("(select")
                boundSql.append("  min(").append(partKey).append(") as minKey ")
                    .append(", max(").append(partKey).append(") as maxKey ")
                    .append(" from ").append("(" + props.getProperty("JDBCSql") + ")) tmp")
                println(sysTime + "Retriving the lower and upper bounds using the below SQL")
                println(sysTime + boundSql.toString)
                var dframeBounds = sqlContext.read.format("jdbc")
                    .options(dfOptions)
                    .option("dbtable", boundSql.toString)
                    .load()
                lowerBound = dframeBounds.map(t => t(0)).collect()(0).toString
                upperBound = dframeBounds.map(t => t(1)).collect()(0).toString
                sqlContext.emptyDataFrame
                sqlContext.clearCache
            }

            dfOptions += ("dbtable" -> { "(" + props.getProperty("JDBCSql") + ") tmp" })
            dfOptions += ("fetchsize" -> props.getProperty("JDBCFetchSize"))
            dfOptions += ("isolationLevel" -> props.getProperty("JDBCIsolationLevel"))
            if ("" != noOfPartitions) {
                dfOptions += ("numPartitions" -> noOfPartitions)
            }
            if ("" != lowerBound && "" != upperBound) {
                println(sysTime + "Forcing lowerBound:" + lowerBound + " and upperBound: " + upperBound + "\n")
                dfOptions += ("lowerBound" -> lowerBound)
                dfOptions += ("upperBound" -> upperBound)
            }

            val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
            import hiveContext.sql
            import hiveContext.implicits._
            println(sysTime + "All Props:" + props + "\n")
            println(sysTime + "DataFrame options: " + dfOptions + "\n")
            println(sysTime + "Save mode: " + mode + " Target Table: " + hiveSchemaTable)

            val tbls = for (tbl <- hiveContext.sql("show tables in " + hiveSchema).map(t => t(0)).collect()) yield tbl.asInstanceOf[String].toUpperCase

            if (tbls.contains(hiveTable)) {
                val x = hiveContext.sql("select count(*) as Recs_before_load from " + hiveSchemaTable).map(t => t(0)).collect()(0)
                println(sysTime + "Number of records in '" + hiveSchemaTable + " before load : " + x)
            } else {
                println(sysTime + " Table '" + hiveSchemaTable + "' not found. Forcing the hive load mode to override.")
                mode = org.apache.spark.sql.SaveMode.Overwrite
            }

            println(sysTime + " Building DataFrame")
            val df = hiveContext.read.format("jdbc").options(dfOptions).load()
            println(sysTime + "Number of records to be inserted: " + df.count)
            println(sysTime + "Saving DataFrame to Hive table: " + hiveSchemaTable)
            println(sysTime + "DF schema: ")
            df.printSchema()

            df.write.mode(mode).saveAsTable(hiveSchemaTable)
            val x = hiveContext.sql("select count(*) as Recs_before_load from " + hiveSchemaTable).map(t => t(0)).collect()(0)
            println(sysTime + "Number of records in '" + hiveSchemaTable + " after load : " + x)
        } catch {
            case e: Exception =>
                Seq[String]()
                println(sysTime + e)
                status.put(jobID, -1)
        } finally {
            println(sysTime + "Processing ended")
            Thread.`yield`
        }
    }
}
