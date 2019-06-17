import CommonFuction.{SEP, filterLog, formatUserLog, getYesterday}
import CommonObj.users
import org.apache.spark.sql.{SaveMode, SparkSession}

object test_func {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("readCSV").getOrCreate

    import spark.implicits._
    //此处需要添加对一些基本的不符合条件的日志过滤掉
    //val newsLogDataFrame = spark.read.textFile("E:/test/recommend-system/journal/userlog/2019-05-31/").rdd.map(formatUserLog).filter(filterLog).toDF().where("un = '' and rcc !=''")
    //newsLogDataFrame.show(20)

    println("".isEmpty)

    //根据用户浏览日志信息，更新用户文章类别画像
    //val bookLog = newsLogDataFrame.where("ro = 'tushu'").select("un","ri","gki")
    //val value = newsLogDataFrame.groupBy("gki").agg(Map("gki"->"count"))

    /*
    var value = newsLogDataFrame.select("gki").rdd.map(row=>{
      (row.getAs[String]("gki"),1)
    }).groupByKey().map(map=>{
      users(0,map._1,"","",0,0,"","","","","","","","")
    }).toDF()

    value
      .repartition(1)
      .write
      .format("csv")
      //.option("escape","")
      .option("sep",SEP)
      //.option("header",true)
      .mode(SaveMode.Overwrite)
      .save("E:/test/recommend-system/journal/GuestUserPortraitOutput")
    //bookLog.show()
     */



    spark.stop()
  }
}
