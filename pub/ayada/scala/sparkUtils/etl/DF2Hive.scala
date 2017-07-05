package pub.ayada.scala.sparkUtils.etl

import org.apache.spark.sql.DataFrame

case class DF2HiveProps(id: String, srcDF: String, var loadType: String, schema: String, table: String, preLoadCount: Boolean, postLoadCount: Boolean)

object DF2Hive {

    def write2Hive(jobId: String,
                   hiveContext: org.apache.spark.sql.hive.HiveContext,
                   dF2HiveProps: pub.ayada.scala.sparkUtils.etl.DF2HiveProps,
                   df: (DataFrame, Boolean)): Unit = {
        val schemaTable = dF2HiveProps.schema + "." + dF2HiveProps.table
        val jobID = "DF2Hive" + jobId
        if (dF2HiveProps.preLoadCount && isTableAvailable(jobID, hiveContext, dF2HiveProps)) {
            rptHiveRecordCount(jobID, " before load: ", hiveContext, dF2HiveProps)
        }
        if (df._2) {
            println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobID) + "Number of records to be inserted: " + df._1.count + " from :" + dF2HiveProps.srcDF)
        }

        df._1.write.mode(if ("overwrite" == dF2HiveProps.loadType)
            org.apache.spark.sql.SaveMode.Overwrite
        else org.apache.spark.sql.SaveMode.Append).saveAsTable(schemaTable)

        try { df._1.unpersist() }
        catch { case t: Throwable => }

        hiveContext.refreshTable(schemaTable)

        if (dF2HiveProps.postLoadCount) {
            rptHiveRecordCount(jobID, " after load: ", hiveContext, dF2HiveProps)
        }

    }
    /**
     * Extracts the jobinfo related to hive load from the input xml node object. <br>
     * If a property is defined in both propsFile and in the xml, the value in the xml will take precedence
     * <br><br>List of properties, <code>
     * <br>*2hive \ "hive" \ "@loadType"
     * <br>*2hive \ "hive" \ "@schema"
     * <br>*2hive \ "hive" \ "@table"
     * <br>*2hive \ "hive" \ "@preLoadCount" &nbsp;&nbsp; (default:false)
     * <br>*2hive \ "hive" \ "@postLoadCount" &nbsp;&nbsp; (default:false)
     * <code>
     */
    def getDF2HiveProps(jobxmlNode: scala.xml.Node): List[DF2HiveProps] = {
        var hProps: List[DF2HiveProps] = List()
        (jobxmlNode \ "hive").foreach(h => {
            val HiveTable = (h \ "@table").text.trim.toUpperCase

            if ("" != HiveTable) {
                val HiveLoadId = (h \ "@id").text.trim
                val HiveSrcDF = (h \ "@srcDF").text.trim
                val HiveLoadType = (h \ "@loadType").text.trim.toLowerCase
                val HiveSchema = (h \ "@schema").text.trim.toUpperCase
                val HivePreLoadCount = ("true" == (h \ "@preLoadCount").text.trim.toLowerCase)
                val HivePostLoadCount = ("true" == (h \ "@postLoadCount").text.trim.toLowerCase)
                hProps = hProps :+ new DF2HiveProps(HiveLoadId, HiveSrcDF, HiveLoadId, { if ("" == HiveSchema) "DEFAULT" else HiveSchema }, HiveTable, HivePreLoadCount, HivePostLoadCount)
            } else {
                println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-DF2Hive") +
                    "Err: Table name is blank. Ignoring the hive load:\n" + new scala.xml.PrettyPrinter(250, 4).format(h))
            }
        })

        /*        var i = 0
        val res = for (h <- hProps) yield { i += 1; "\t\t\t%3d.".format(i) + (h.toString) + "\n" }
        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-DF2Hive") + "Hive Load options: \n" + res.mkString(" , "))
*/
        hProps
    }

    def isTableAvailable(jobPropsId: String, hiveContext: org.apache.spark.sql.hive.HiveContext, dF2HiveProps: DF2HiveProps): Boolean = {

        val sql = "show tables in " + dF2HiveProps.schema
        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobPropsId) + "Checking tables using sql : " + sql)
        val tblDF1 = hiveContext.sql(sql)
        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobPropsId) + "Number of tables found: " + tblDF1.count())
        val tblDF = tblDF1.map(t => t(0)).collect()

        val tbls = for (tbl <- tblDF)
            yield tbl.asInstanceOf[String].toUpperCase

        val schemaTable = new StringBuilder(dF2HiveProps.schema).append(".").append(dF2HiveProps.table).toString

        if (tbls.contains(dF2HiveProps.table)) {
            return true
        } else {
            println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobPropsId) + "Table '" + schemaTable + "' not found. Forcing the hive load mode to Overwrite.")
            dF2HiveProps.loadType = "overwrite"
        }
        false
    }

    def rptHiveRecordCount(jobPropsId: String, step: String, hiveContext: org.apache.spark.sql.hive.HiveContext, dF2HiveProps: DF2HiveProps) = {
        val schemaTable = new StringBuilder(dF2HiveProps.schema).append(".").append(dF2HiveProps.table).toString
        val countSQL = "select count(*) as cnt from " + schemaTable
        val x = hiveContext.sql(countSQL)
            .map(t => t(0))
            .collect()(0)
        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobPropsId) +
            "Number of records in " + schemaTable + step + x)

    }

}
