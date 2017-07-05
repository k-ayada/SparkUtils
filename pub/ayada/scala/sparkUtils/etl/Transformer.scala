package pub.ayada.scala.sparkUtils.etl

import org.apache.spark.sql.DataFrame

//import org.apache.spark.sql.DataFrame

case class TransformerProps(id: String, srcDF: String, typ: String, registerTempTable: Boolean, cache: Boolean, persist: Boolean, loadCount: Boolean, printSchema: Boolean, conditionExpr: String)

object Transformer {
    def getTransformerProps(xFerNode: scala.xml.Node): TransformerProps = {
        val id = (xFerNode \ "@id").text.trim
        val srcDF = (xFerNode \ "@srcDF").text.trim
        val typ = (xFerNode \ "@type").text.trim
        val registerTempTable = ("true" == (xFerNode \ "@registerTempTable").text.trim.toLowerCase)
        val cache = ("true" == (xFerNode \ "@cache").text.trim.toLowerCase)
        val persist = ("true" == (xFerNode \ "@persist").text.trim.toLowerCase)
        val loadCount = ("true" == (xFerNode \ "@loadCount").text.trim.toLowerCase)
        val printSchema = ("true" == (xFerNode \ "@printSchema").text.trim.toLowerCase)
        val conditionExpr = xFerNode.text.trim
        TransformerProps(id, srcDF, typ, registerTempTable, cache, persist, loadCount, printSchema, conditionExpr)
    }
}
