import CommonFuction.{SEP, filterLog, formatUserLog, getYesterday}
import CommonObj.users
import org.apache.spark.sql.{SaveMode, SparkSession}

object test_func {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("readCSV").getOrCreate

    import spark.implicits._
    //此处需要添加对一些基本的不符合条件的日志过滤掉.where("un = '' and rcc !=''")

    //val logObject = spark.sparkContext.textFile("E:/test/recommend-system/journal/userlog/2019-06-30/").map(formatUserLog).filter(filterLog)

    val logObject = spark.read.textFile("E:/test/recommend-system/journal/userlog/2019-06-30/").rdd.map(formatUserLog).filter(filterLog)


    logObject.toDF().show(100)


    //println(value.rdd.map(row=>row.getAs[String]("un")).groupBy(x=>x).count())





    //newsLogDataFrame.where("un = ''").count()


    //var bookLog = newsLogDataFrame.where("ro = 'tushu'").select("un","ri","gki")


    //val frame = newsLogDataFrame.groupBy("ro").agg(Map("ro"->"count"))

    //frame.show(20)

    //newsLogDataFrame.


    //newsLogDataFrame.show(20)

    //根据用户浏览日志信息，更新用户文章类别画像
    //val bookLog = newsLogDataFrame.where("ro = 'tushu'").select("un","ri","gki")
    //val value = newsLogDataFrame.groupBy("gki").agg(Map("gki"->"count"))

    /*


    val uaArray = frame.rdd.map(row=>{row.getAs[String]("ua")}).collect()

    frame.show()

    val newsLogDataFrame = logObject.filter(row=> !uaArray.contains(row.ua)).toDF()

    newsLogDataFrame.cache()


    println("总浏览次数："+newsLogDataFrame.count(),frame.count())

    println("总浏览人数"+newsLogDataFrame.groupBy("gki").agg(Map("gki"->"count")).count())

    println("登录用户总浏览人数"+newsLogDataFrame.where("un != ''").groupBy("un").agg(Map("un"->"count")).count())

    //println("总浏览人数"+newsLogDataFrame.groupBy("gki").agg(Map("gki"->"max")).count())

    //val value = newsLogDataFrame.where("un != ''")

    //value.groupBy("un").agg(Map("ro"->"count")).show()

    //val newFrame = value.groupBy("un").agg(Map("ro"->"count"))

    //newFrame.orderBy(newFrame("count(ro)").desc).show(20)

    //println(value.groupBy("un").agg(Map("ro"->"count")).count())
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
        logObject.cache()

    val uaTuples = logObject.toDF().groupBy("ua", "ci").agg(Map("ua" -> "count")).where("count(ua)>200").rdd.map(row => {
      (row.getAs[String]("ua"), row.getAs[String]("ci"))
    }).collect()



    val frame = logObject.toDF().groupBy("ua","ci").agg(Map("ua"->"count"))

    frame.show()

    val newsLogDataFrame = logObject.filter(row=>{
      var res = true
      import scala.util.control.Breaks._
      breakable{
        for(tuple <- uaTuples){
          res = !(tuple._1.equals(row.ua) && tuple._2.equals(row.ci))
          if(!res) break
        }
      }
      res
    }).toDF()

    newsLogDataFrame.cache()


    println("总浏览次数："+newsLogDataFrame.count(),frame.count())

    println("总浏览人数"+newsLogDataFrame.groupBy("gki").agg(Map("gki"->"count")).count())

    println("登录用户总浏览人数"+newsLogDataFrame.where("un != ''").groupBy("un").agg(Map("un"->"count")).count())
     */



    spark.stop()
  }
}
