import CommonFuction.{SEP, filterLog, formatUserLog}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 格式化用户日志为parquet格式，并保存
  */
object FormatUserLog {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(FormatUserLog.getClass.getSimpleName)
      .getOrCreate
    import spark.implicits._
    //此处需要添加对一些基本的不符合条件的日志过滤掉
    val newsLogDataFrame = spark.read.textFile(args(0)).rdd.map(formatUserLog).filter(filterLog).toDF()

    println("-------------------------------格式化日志正在保存，保存位置："+args(1))

    newsLogDataFrame
      .write
      .mode(SaveMode.Overwrite)
      .parquet(args(1))
  }
}
