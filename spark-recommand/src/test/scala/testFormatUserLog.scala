import common.{SEP, filterLog, formatUserLog}
import org.apache.spark.sql.{SaveMode, SparkSession}

object testFormatUserLog {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("testFormatUserLog").getOrCreate

    import spark.implicits._
    //此处需要添加对一些基本的不符合条件的日志过滤掉
    val newsLogDataFrame = spark.read.textFile("E:/test/recommend-system/journal/userlog/").rdd.map(formatUserLog).filter(filterLog).toDF()

    val articleLog = newsLogDataFrame.where("un is not null and un != '' and ro = 'article' and rcc !=''")
    println("原始数据浏览量："+newsLogDataFrame.count()+"\n"+"符合条件的日志浏览量"+articleLog.count())

    /**

    articleLog.repartition(1)
      .write
      .format("csv")
      //.option("escape","")
      .option("sep",SEP)
      //.option("header",true)
      .mode(SaveMode.Overwrite)
      .save("E:/test/testFormatUserLog")
      */



    spark.stop()
  }
}
