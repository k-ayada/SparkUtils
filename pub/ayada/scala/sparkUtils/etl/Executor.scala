package pub.ayada.scala.sparkUtils.etl

import org.apache.spark.sql.DataFrame
import org.apache.commons.configuration.PropertiesConfiguration

class Executor(sparkContext: org.apache.spark.SparkContext,
               jobProps: pub.ayada.scala.sparkUtils.etl.JobProps,
               status: scala.collection.mutable.HashMap[String, Int],
               maxThread: Int) {
    private lazy val pool = java.util.concurrent.Executors.newFixedThreadPool(maxThread)
    val jobId = "Executor-" + jobProps.id
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.sql
    import sqlContext.implicits._
    import org.apache.spark.sql._

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)
    import hiveContext.sql
    import hiveContext.implicits._

    def process() {
        try {
            var dfs: scala.collection.mutable.Map[String, (DataFrame, Boolean)] = scala.collection.mutable.Map()

            for (r <- jobProps.readProps) {
                r.getClass.getName match {
                    case "pub.ayada.scala.sparkUtils.Jdbc2DFProps" => {
                        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobId) + "HiveContext: " + hiveContext.toString)
                        new pub.ayada.scala.sparkUtils.etl.Jdbc2DF(sqlContext, hiveContext, r.asInstanceOf[pub.ayada.scala.sparkUtils.etl.Jdbc2DFProps], dfs).execute
                    }
                    case "pub.ayada.scala.sparkUtils.Csv2DFProps" => {
                        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobId) + "HiveContext: " + hiveContext.toString)
                        //pool.execute(new pub.ayada.scala.sparkUtils.Csv2DF(hiveContext, r.asInstanceOf[Csv2DFProps], dfs))
                        new pub.ayada.scala.sparkUtils.etl.Csv2DF(hiveContext, r.asInstanceOf[pub.ayada.scala.sparkUtils.etl.Csv2DFProps], dfs).execute
                    }
                    case _ =>
                        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobId) + "Err:Task ignored as the associated load type is currently not supported. Type:" + r.toString())
                }
            }

            pool.shutdown();
            while (!pool.isTerminated()) {
                Thread.sleep(5000)
            }

            for (r <- jobProps.readProps) {
                r.getClass.getName match {
                    case "pub.ayada.scala.sparkUtils.Jdbc2DFProps" => {
                        val id = r.asInstanceOf[Jdbc2DFProps].id
                        val tup = dfs.getOrElse(id, null)
                        if (null != tup) {
                            tup._1.registerTempTable(id)
                        }
                    }
                    case "pub.ayada.scala.sparkUtils.Csv2DFProps" => {
                        val id = r.asInstanceOf[Csv2DFProps].id
                        val tup = dfs.getOrElse(id, null)
                        if (null != tup) {
                            tup._1.registerTempTable(id)
                        }
                    }
                }
            }

            if (dfs.size > 0) {
                runTransforms(jobProps.id, jobProps.transformProps, dfs)
                println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobId) + "List of DFs: " + dfs.keySet.mkString(","))
                storeAndUnpersitDFs(dfs, jobProps.dF2HiveProps)
                unpersitDFs(dfs, jobProps.transformProps)
            } else {
                println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobId) + "Err: Nothing to load as no DataFrames were created.")
            }

        } catch {
            case e: Exception =>
                val sb = new StringBuilder(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobId))
                sb.append(e).append("\nStackTrace:\n")
                for (element: StackTraceElement <- e.getStackTrace()) {
                    sb.append(element.toString());
                    sb.append("\n");
                }
                println(sb.toString)
                status.put(jobProps.id, 1)
        } finally {
            println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobId) + "Processing ended")
            Thread.`yield`
        }
    }

    def runTransforms(jobId: String, props: List[pub.ayada.scala.sparkUtils.etl.TransformerProps],
                      dfs: scala.collection.mutable.Map[String, (org.apache.spark.sql.DataFrame, Boolean)]) = {
        props.foreach(prop => {
            val df = dfs.getOrElse(prop.srcDF, null)
            val taskID = jobId + "-" + prop.id

            if (null != df && df._1 != null) {
                println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Transformer-" + taskID) + "Running Transformarmation: " + pub.ayada.scala.utils.ObjectUtils.prettyPrint(prop))
                if (df._2) {
                    println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Transformer-" + taskID) + "Number of records in the DF: " + prop.srcDF + " is " + df._1.count)
                    dfs.update(prop.srcDF, (df._1, false))
                }
                val df2 = prop.typ match {
                    case "filter" => {

                        df._1.filter(prop.conditionExpr).alias(prop.id)
                    }
                    case "sql" => {
                        hiveContext.sql(prop.conditionExpr).alias(prop.id)
                    }
                    case _ => throw new Exception("Unknown transformation type received (valid:filter|sql) : " + prop.typ)
                }
                println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Transformer-" + taskID) + " Registering DF as " + prop.id)
                try {

                    df2.registerTempTable(prop.id)
                } catch {
                    case t: Throwable => {
                        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Transformer-" + taskID) + " Failed to register " + prop.id)
                        t.printStackTrace()
                    }
                }
                println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Transformer-" + taskID) + "Number of records in the DF: " + prop.srcDF + " is " + df2.count)

                if (prop.printSchema) {
                    println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Transformer-" + taskID) + "DF schema of : " + prop.id + "\n\t\t" + df2.schema.simpleString)
                }
                if (prop.cache) {
                    println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Transformer-" + taskID) + " Caching the DF " + prop.id)
                    df2.cache()
                }
                if (prop.persist) {
                    println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Transformer-" + taskID) + " Persisting the DF " + prop.id + " with StorageLevel.MEMORY_AND_DISK")
                    df2.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
                }

                dfs.put(prop.id, (df2, prop.loadCount))

            } else {
                println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Transformer-" + taskID) + "Wrn: Empty DataFrame: " + prop.srcDF)
            }

        })
    }

    def storeAndUnpersitDFs(dfs: scala.collection.mutable.Map[String, (DataFrame, Boolean)],
                            dF2HiveProps: List[pub.ayada.scala.sparkUtils.etl.DF2HiveProps]) {
        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobId) + "Starting Hive load. Number tables to load:" + dF2HiveProps.size)
        dF2HiveProps.foreach(hp => {
            println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobId + "-" + hp.srcDF) + "Preparing for hive load id:" + hp.id + ", Source DF:" + hp.srcDF)

            val d: (DataFrame, Boolean) = dfs.getOrElse(hp.srcDF, (null, false))
            if (d._1 == null) {
                println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobId + "-" + hp.srcDF) + "Err: '" + hp.srcDF + "' is null. Not writing to table : " + hp.schema + "." + hp.table)
            } else {
                DF2Hive.write2Hive(jobId + "-" + hp.srcDF, hiveContext, hp, d)
                dfs.update(hp.srcDF, (d._1, false))
            }
        })
    }

    def unpersitDFs(dfs: scala.collection.mutable.Map[String, (DataFrame, Boolean)],
                    transformProps: List[pub.ayada.scala.sparkUtils.etl.TransformerProps]) {
        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobId) + "Starting to unpersist DataFrames")
        transformProps.foreach(p => {
            val df: (DataFrame, Boolean) = dfs.getOrElse(p.id, (null, false))
            if (df != null) {
                try {
                    println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobId) + "Unpersisting: " + p.id)
                    df._1.unpersist()
                } catch { case t: Throwable => }
            }
        })
    }
}
