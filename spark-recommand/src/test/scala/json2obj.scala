import org.apache.spark.sql.SparkSession
import common._

object json2obj {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("json2map").getOrCreate

    var lines = spark.read.textFile("E:/test/recommend-system/journal/info.log").rdd
    import spark.implicits._
    val proDataFrame = spark.read.textFile("E:/test/recommend-system/journal/journalbaseinfo_temp/").rdd.map(formatJournalBaseInfo).toDF()

    val logDataFrame = lines.map(formatUserLog).toDF()

    val logTempDataFrame = logDataFrame.filter("ro = 'journal' and un is not null and un != ''")

    logTempDataFrame
      .join(proDataFrame,proDataFrame("PYKM") === logDataFrame("ri"))
      .select(
        proDataFrame("title"),
        proDataFrame("module_id"),
        proDataFrame("keywords"),
        logDataFrame("un"),
        logDataFrame("vt")
      ).show()

    spark.stop()
  }
}
