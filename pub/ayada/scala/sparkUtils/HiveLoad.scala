package pub.ayada.scala.sparkUtils

import java.util.concurrent.Executors
import scala.concurrent._
import pub.ayada.scala.sparkUtils.HiveLoad._
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Semaphore
import java.time.Duration
class HiveLoad(args: Array[String]) {
    private val dtFmt = new java.text.SimpleDateFormat("yyyyMMdd hh:mm:ss ")
    private def sysTime: String = {
        val sb: StringBuilder = new StringBuilder()
        sb.append("[")
            .append(dtFmt.format(new java.util.Date()))
            .append("][Driver] ")
        sb.toString
    }

    @throws(classOf[Exception])
    private val jobConfXML: scala.xml.Elem = {
        val arg = args.sliding(2, 1).toList
        var xml: scala.xml.Elem = null
        args.sliding(2, 1).toList.collect {
            case Array("--JobConf", jobConf: String) =>
                println(sysTime + "--JobConf: " + jobConf + "\n")
                val reader = new java.io.InputStreamReader(pub.ayada.scala.utils.hdfs.HdfsFileIO.getHdfsFlInputStream(jobConf), "UTF-8")
                xml = scala.xml.XML.load(reader)
        }
        if (xml == null) {
            throw new java.lang.Exception(sysTime + "Failed to parse the commanline. Need the parameter --JobConf \"<path to xml file>\" ")
        }
        xml
    }
    private val AppName = (jobConfXML \\ "@appName").text
    private val maxThread = (jobConfXML \\ "@maxNumberOfThreads").text.toInt
    private val pool = java.util.concurrent.Executors.newFixedThreadPool(maxThread)
    private lazy val jobProps: List[java.util.Properties] = {
        var jobProps: List[java.util.Properties] = List()

        (jobConfXML \\ "jobs" \\ "jdbc2hive")
            .foreach(jdbc2hive => {
                jobProps = jobProps :+ handleJDBCJobId(jdbc2hive)
            })
        jobProps
    }
    lazy val sc: org.apache.spark.SparkContext = {
        val cnf = new org.apache.spark.SparkConf()
        cnf.setAppName(AppName)
            .set("spark.scheduler.mode", "FAIR")
            .set("spark.driver.allowMultipleContexts", "true")
        val sc = new org.apache.spark.SparkContext(cnf)
        sc.setLogLevel("ERROR")
        sc
    }
    def kickoffLoad(): Int = {

        val status = new scala.collection.mutable.HashMap[String, Int]()

        jobProps.foreach { p =>
            println(sysTime + "Launching JobID : " + p.getProperty("jobId") + "\n")
            status.put(p.getProperty("jobId"), 0)
            pool.execute(new pub.ayada.scala.sparkUtils.Jdbc2Hive(sc, p, status))
        }
        pool.shutdown();
        while (!pool.isTerminated()) {
            Thread.sleep(5000)
        }

        var rc = 0
        for ((k, v) <- status) {
            println(sysTime + "RC for job : " + k + " is : " + v)
            if (0 != v) rc = 0
        }
        rc
    }

    private def handleJDBCJobId(jobxmlNode: scala.xml.Node): java.util.Properties = {

        val printer = new scala.xml.PrettyPrinter(250, 2)
        println(sysTime + printer.format(jobxmlNode) + "\n")

        var JobProps: java.util.Properties = new java.util.Properties

        val JDBCPropsFile = (jobxmlNode \ "jdbc" \ "@propsFile").text.trim
        val JDBCSchema = (jobxmlNode \ "jdbc" \ "@schema").text.trim
        val JDBCTable = (jobxmlNode \ "jdbc" \ "@table").text.trim
        val JDBCFetchSize = (jobxmlNode \ "jdbc" \ "@fetchSize").text.trim
        val JDBCIsolationLevel = (jobxmlNode \ "jdbc" \ "@isolationLevel").text.trim
        val JDBCSql = (jobxmlNode \ "jdbc" \ "sql").text.trim

        val HiveLoadType = (jobxmlNode \ "hive" \ "@loadType").text.trim
        val HiveSchema = (jobxmlNode \ "hive" \ "@schema").text.trim.toUpperCase
        val HiveTable = (jobxmlNode \ "hive" \ "@table").text.trim.toUpperCase
        val HivePartitionKey = (jobxmlNode \ "hive" \ "@partitionKey").text.trim
        val HiveNoOfPartitions = (jobxmlNode \ "hive" \ "@noOfPartitions").text.trim
        val HiveLowerBound = (jobxmlNode \ "hive" \ "@lowerBound").text.trim
        val HiveUpperBound = (jobxmlNode \ "hive" \ "@upperBound").text.trim

        if ("" != JDBCPropsFile) {
            JobProps.putAll(pub.ayada.scala.utils.hdfs.HdfsFileIO.getProps(JDBCPropsFile))
        }

        JobProps.setProperty("jobId", (jobxmlNode \ "@id").text.trim)
        JobProps.setProperty("JDBCPropsFile", JDBCPropsFile)
        JobProps.setProperty("JDBCSchema", JDBCSchema)
        JobProps.setProperty("JDBCTable", JDBCTable)
        JobProps.setProperty("JDBCIsolationLevel", if ("" != JDBCIsolationLevel) JDBCIsolationLevel else "READ_COMMIT")
        JobProps.setProperty("JDBCFetchSize", if ("" != JDBCFetchSize) JDBCFetchSize else "100000")
        JobProps.setProperty("JDBCSql",
            if ("" != JDBCSql)
                JDBCSql
            else {
                println(sysTime + "Custom SQL not provided. Using the input schema and table to build the sql.\n")
                if ("" != JDBCSchema) {
                    "select * from " + JDBCSchema + "." + JDBCTable
                } else {
                    "select * from " + JDBCTable
                }
            })

        JobProps.setProperty("HiveLoadType", HiveLoadType)
        JobProps.setProperty("HiveSchema", HiveSchema)
        JobProps.setProperty("HiveTable", HiveTable)
        JobProps.setProperty("HivePartitionKey", HivePartitionKey)
        JobProps.setProperty("HiveNoOfPartitions", HiveNoOfPartitions)
        JobProps.setProperty("HiveLowerBound", HiveLowerBound)
        JobProps.setProperty("HiveUpperBound", HiveUpperBound)

        println(sysTime + "JDBC Properties for job: " + JobProps.getProperty("jobId") + "\n" + JobProps + "\n")
        JobProps
    }
}

object HiveLoad {
    def main(args: Array[String]): Unit = {
        val run = args(0)
        var rc = 0
        run match {
            case "jdbc2Hive" => {
                println("Load Type: " + run)
                val hl: HiveLoad = new HiveLoad(args)
                rc = hl.kickoffLoad
            }
            case _ => throw new Exception("Please pass valid value for arument 1. Options: jdbc2Hive , csv2Hive ")
        }
        System.exit(rc)
    }
}
