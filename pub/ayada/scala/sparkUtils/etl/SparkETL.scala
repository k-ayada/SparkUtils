package pub.ayada.scala.sparkUtils.etl
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Semaphore
import java.util.concurrent.Executors
import scala.xml.Elem
import scala.concurrent._
import pub.ayada.scala.sparkUtils.etl.SparkETL._
import pub.ayada.scala.utils.DateTimeUtils._

class SparkETL(args: Array[String]) {
    /*
     * xml DOM object of the input xml file. Used for running the xpath queries
     */
    @throws(classOf[Exception])
    private lazy val jobConfXML: scala.xml.Elem = {
        val arg = args.sliding(2, 1).toList
        var xml: scala.xml.Elem = null
        args.sliding(2, 1).toList.collect {
            case Array("--JobConf", jobConf: String) =>
                println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + "--JobConf: " + jobConf)
                val reader = new java.io.InputStreamReader(pub.ayada.scala.utils.hdfs.HdfsFileIO.getHdfsFlInputStream(jobConf), "UTF-8")
                xml = scala.xml.XML.load(reader)
        }
        if (xml == null) {
            throw new java.lang.Exception(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + "Failed to parse the commanline. Need the parameter --JobConf \"<path to xml file>\" ")
        }
        xml
    }
    private lazy val AppName = (jobConfXML \ "@appName").text
    private lazy val maxThread = (jobConfXML \ "@maxNumberOfThreads").text.toInt
    /*
     * Spark Context
     */
    private lazy val sparkContext: org.apache.spark.SparkContext = {
        val cnf = new org.apache.spark.SparkConf()
        cnf.setAppName(AppName)
            .set("spark.scheduler.mode", "FAIR")
            .setIfMissing("hive.execution.engine", "spark")
        //.set("spark.driver.allowMultipleContexts", "true")

        val sc = new org.apache.spark.SparkContext(cnf)
        sc.setLogLevel("ERROR")
        sc
    }
    /*
     * List of properties passed in the input properties file provided in the
     * config file
     */
    private lazy val jobProps: List[pub.ayada.scala.sparkUtils.etl.JobProps] = {

        var jobProps: List[pub.ayada.scala.sparkUtils.etl.JobProps] = List()
        val printer = new scala.xml.PrettyPrinter(250, 4)

        (jobConfXML \ "_").foreach(job => {
            val sb = new StringBuilder()
            sb.append(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + " Job: " + (job \ "@id").text.trim + " LoadType: " + job.label).append("\n")
            sb.append(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + " Input Config:\n" + printer.format(job))

            val jobId = (job \ "@id").text.trim
            job.label match {
                case "jdbc2hive" => {
                    val h = pub.ayada.scala.sparkUtils.etl.DF2Hive.getDF2HiveProps(job)
                    if (h.size == 0) {
                        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + "Err: Failed to retrive properties to persit final data in Hive. Skippig the ETL. JobID:" + jobId)
                    } else {
                        val p = pub.ayada.scala.sparkUtils.etl.Jdbc2DF.getJdbc2DFProps(job, sparkContext)
                        val t = getTransformers((job \ "transformers"))
                        jobProps = jobProps :+ pub.ayada.scala.sparkUtils.etl.JdbcJobProps(id = jobId, readProps = p, dF2HiveProps = h, transformProps = t)
                        sb.append("\n").append(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + "JDBC Properties for job: " + jobId + "\n" + pub.ayada.scala.utils.ObjectUtils.prettyPrint(p))
                        sb.append("\n").append(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + "Transformation Properties for job: " + jobId + "\n" + pub.ayada.scala.utils.ObjectUtils.prettyPrint(for (x <- t) yield { x }))
                        sb.append("\n").append(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + "Hive Properties for job: " + jobId + "\n" + pub.ayada.scala.utils.ObjectUtils.prettyPrint(h))
                    }
                }
                case "csv2hive" => {
                    val h = pub.ayada.scala.sparkUtils.etl.DF2Hive.getDF2HiveProps(job)
                    if (h.size == 0) {
                        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + "Err: Failed to retrive properties to persit final data in Hive. Skippig the ETL. JobID:" + jobId)
                    } else {
                        val p = pub.ayada.scala.sparkUtils.etl.Csv2DF.getCsv2DFProps(job)
                        val t = getTransformers((job \ "transformers"))
                        jobProps = jobProps :+ pub.ayada.scala.sparkUtils.etl.CsvJobProps(id = jobId, readProps = p, dF2HiveProps = h, transformProps = t)
                        sb.append("\n").append(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + "CSV Properties for job: " + jobId + "\n" + pub.ayada.scala.utils.ObjectUtils.prettyPrint(p))
                        sb.append("\n").append(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + "Transformation Properties for job: " + jobId + "\n" + pub.ayada.scala.utils.ObjectUtils.prettyPrint(t))
                        sb.append("\n").append(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + "Hive Properties for job: " + jobId + "\n" + pub.ayada.scala.utils.ObjectUtils.prettyPrint(h))
                    }
                }
                case _ => {
                    sb.append("\n").append("Err:Skipping the job:").append(jobId).append(". Reason: Unknown Job type").append(job.label).append("received. Valid values : jdbc2hive | csv2hive")
                }
            }
            println(sb.toString)
        })

        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + "# jobs to process: " + jobProps.size)
        jobProps
    }

    def getTransformers(xferNode: scala.xml.NodeSeq): List[pub.ayada.scala.sparkUtils.etl.TransformerProps] = {
        var res: List[pub.ayada.scala.sparkUtils.etl.TransformerProps] = List()
        //println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + "Transformers:" + new scala.xml.PrettyPrinter(250, 4).formatNodes(xferNode))
        (xferNode \ "transformer").foreach(x => {
            res = res :+ pub.ayada.scala.sparkUtils.etl.Transformer.getTransformerProps(x)
        })
        res
    }

    /**
     * Starts off the load process.
     */
    def kickoffLoad(): Int = {
        if (sparkContext == null) {
            throw new Exception("Spark Context is not yet initialized... :(")
        }

        val status = new scala.collection.mutable.HashMap[String, Int]()
        jobProps.foreach { p =>
            println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + "Processing JobID : " + p.id)
            status.put(p.id, 0)
            val j = new pub.ayada.scala.sparkUtils.etl.Executor(sparkContext, p, status, maxThread)
            j.process()
        }

        var rc = 0
        for ((k, v) <- status) {
            println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + "RC for job : " + k + " is : " + v)
            if (0 != v) rc += v
        }
        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main") + "Final RC is : " + rc)
        rc
    }
}

object SparkETL {
    def main(args: Array[String]): Unit = {
        val run = args(0)
        var rc = 0
        println("Load Type: " + run)
        run match {
            case "jdbc2Hive"     => {}
            case "csv2Hive"      => {}
            case "multiSrc2Hive" => {}
            case _               => throw new Exception("Please pass valid value for arument 1. Options: jdbc2Hive | csv2Hive | multirc2Hive")
        }

        val sparkETL: SparkETL = new SparkETL(args)
        rc = sparkETL.kickoffLoad

        if (rc != 0)
            throw new Exception("Failed to complete all the tasks..")
    }
}

/*

spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
val hc = new org.apache.spark.sql.hive.HiveContext(sc)
val dfOptions: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String]("header" -> "true", "delimiter" -> "|", "quote" -> "\"","escape" -> "\\","mode" -> "PERMISSIVE","charset" -> "UTF-8","comment" -> null,"dateFormat" ->  "M/d/yyyy H:m:s a","nullValue" -> "","inferSchema" -> "true")
val inDF = hc.read.format("com.databricks.spark.csv").options(dfOptions).load("/user/krnydc/Bounces.txt")
inDF.registerTempTable("inDF")
inDF.printSchema

 */
