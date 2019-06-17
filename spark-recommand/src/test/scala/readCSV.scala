import org.apache.spark.sql.SparkSession
import CommonFuction._

object readCSV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("readCSV").getOrCreate

    /*
    import spark.implicits._
    val dataFrame = spark
      .read
      .option("sep", ";")
      //.option("header", true)
      .csv("E:\\test\\recommend-system\\journal\\UserPortraitOutput\\")
      .toDF("id","name","prefList","latest_log_time")
    //dataFrame.show(20)
    val userList = dataFrame.rdd.map(row => {
      var pre = row.getAs[String]("prefList")
      var id = row.getAs[String]("id")

      var map = jsonPrefListtoMap(pre)
      var name = row.getAs[String]("name")

      UserTemp(id.toLong, name, pre, map, row.getAs[String]("latest_log_time"))
    })
    userList.collect()*/
    //2.2 获取所有待推荐的新闻列表（格式化所有新闻对应的关键词及关键词的权重）
    val newsList = spark.read.textFile("E:/test/journals-recommand-source/journalbaseinfo_temp.csv").rdd.map(formatNews).collect()
    spark.stop()
  }
}
