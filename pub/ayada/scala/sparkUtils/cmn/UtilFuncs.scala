package pub.ayada.scala.sparkUtils.cmn

object UtilFuncs {
    def literal2SparkDataType(dataType: String): org.apache.spark.sql.types.DataType = {

        dataType.toLowerCase match {
            case "int"           => org.apache.spark.sql.types.DataTypes.IntegerType
            case "integer"       => org.apache.spark.sql.types.DataTypes.IntegerType
            case "intergertype"  => org.apache.spark.sql.types.DataTypes.IntegerType
            case "long"          => org.apache.spark.sql.types.DataTypes.LongType
            case "longtype"      => org.apache.spark.sql.types.DataTypes.LongType
            case "floa"          => org.apache.spark.sql.types.DataTypes.FloatType
            case "floattype"     => org.apache.spark.sql.types.DataTypes.FloatType
            case "double"        => org.apache.spark.sql.types.DataTypes.DoubleType
            case "decimal"       => org.apache.spark.sql.types.DataTypes.DoubleType
            case "doubletype"    => org.apache.spark.sql.types.DataTypes.DoubleType
            case "bool"          => org.apache.spark.sql.types.DataTypes.BooleanType
            case "boolean"       => org.apache.spark.sql.types.DataTypes.BooleanType
            case "booleantype"   => org.apache.spark.sql.types.DataTypes.BooleanType
            case "nulltype"      => org.apache.spark.sql.types.DataTypes.NullType
            case "byte"          => org.apache.spark.sql.types.DataTypes.ByteType
            case "bytetype"      => org.apache.spark.sql.types.DataTypes.ByteType
            case "datetime"      => org.apache.spark.sql.types.DataTypes.TimestampType
            case "timestamp"     => org.apache.spark.sql.types.DataTypes.TimestampType
            case "timestamptype" => org.apache.spark.sql.types.DataTypes.TimestampType
            case "date"          => org.apache.spark.sql.types.DataTypes.DateType
            case "datetype"      => org.apache.spark.sql.types.DataTypes.DateType
            case _               => org.apache.spark.sql.types.DataTypes.StringType
        }

    }
    def sql2SparkDataType(sqlType: Int): org.apache.spark.sql.types.DataType = {
        sqlType match {
            case java.sql.Types.INTEGER   => org.apache.spark.sql.types.DataTypes.IntegerType
            case java.sql.Types.BOOLEAN   => org.apache.spark.sql.types.DataTypes.BooleanType
            case java.sql.Types.SMALLINT  => org.apache.spark.sql.types.DataTypes.ShortType
            case java.sql.Types.TINYINT   => org.apache.spark.sql.types.DataTypes.ByteType
            case java.sql.Types.BIGINT    => org.apache.spark.sql.types.DataTypes.LongType
            case java.sql.Types.DECIMAL   => org.apache.spark.sql.types.DataTypes.createDecimalType
            case java.sql.Types.FLOAT     => org.apache.spark.sql.types.DataTypes.FloatType
            case java.sql.Types.DOUBLE    => org.apache.spark.sql.types.DataTypes.DoubleType
            case java.sql.Types.DATE      => org.apache.spark.sql.types.DataTypes.DateType
            case java.sql.Types.TIME      => org.apache.spark.sql.types.DataTypes.TimestampType
            case java.sql.Types.TIMESTAMP => org.apache.spark.sql.types.DataTypes.TimestampType
            case java.sql.Types.VARCHAR   => org.apache.spark.sql.types.DataTypes.StringType
            case _                        => org.apache.spark.sql.types.DataTypes.StringType
        }
    }

}
