package pub.ayada.scala.sparkUtils.etl

trait JobProps {
    def taskType: String
    def id: String
    def dF2HiveProps: List[DF2HiveProps]
    def transformProps: List[pub.ayada.scala.sparkUtils.etl.TransformerProps]
    def readProps: List[Any]
}
case class CsvJobProps(taskType: String = "csv2hive",
                       id: String,
                       dF2HiveProps: List[DF2HiveProps],
                       transformProps: List[pub.ayada.scala.sparkUtils.etl.TransformerProps],
                       override val readProps: List[pub.ayada.scala.sparkUtils.etl.Csv2DFProps]) extends JobProps
case class JdbcJobProps(taskType: String = "jdbc2hive",
                        id: String,
                        dF2HiveProps: List[DF2HiveProps],
                        transformProps: List[pub.ayada.scala.sparkUtils.etl.TransformerProps],
                        override val readProps: List[pub.ayada.scala.sparkUtils.etl.Jdbc2DFProps]) extends JobProps
