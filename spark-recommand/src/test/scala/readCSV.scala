import org.apache.spark.sql.SparkSession
import common._

object readCSV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("readCSV").getOrCreate
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
    userList.collect()
    spark.stop()
  }
}
