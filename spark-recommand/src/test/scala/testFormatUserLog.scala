import common.formatUserLog
import org.apache.spark.sql.SparkSession

object testFormatUserLog {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("json2map").getOrCreate

    var lines = spark.read.textFile("E:/test/recommend-system/journal/info.log").rdd
    import spark.implicits._
    val logDataFrame = lines.map(formatUserLog).toDF()

    logDataFrame.collect()
    spark.stop()
  }
}
