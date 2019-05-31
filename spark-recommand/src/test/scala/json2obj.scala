import org.apache.spark.sql.{SaveMode, SparkSession}
import common._

object json2obj {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("json2map").getOrCreate

    val logTime = spark.sparkContext.broadcast(getNowStr())
    import spark.implicits._
    val userList = spark.read.textFile("E:/test/recommend-system/journal/UserPortraitOutput/").rdd.map(line=>formatUsers(line,logTime)).toDF()

    //var lines = spark.read.textFile("E:/test/recommend-system/bianke_User.csv").rdd


    val userRDD = userList.select("UserName").rdd.collect()

    val usrRddBroadCast = spark.sparkContext.broadcast(userRDD)


    /*根据用户浏览日志更新用户基本信息 **/

    var lines = spark.read.textFile("E:/test/recommend-system/journal/userlog/").rdd


    val logDataFrame = lines.map(formatUserLog).filter(log=>log != null).toDF()

    val logTempDataFrame = logDataFrame.filter("un is not null and un != ''")

    val newUsers = logTempDataFrame.rdd.filter(row => {
      var userName = row.getAs[String](1)
      var bool = if (usrRddBroadCast.value.contains(userName)) false else true
      bool
    }).map(row => {
      var userName = row.getAs[String](1)
      users(0, userName, "", "", 0, 0, "", "", "", "", "", "", "", "")
    }).toDF()


    //根据mysql更新用户信息
    /*
        val biankeUserDataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://master02:3306")
      .option("dbtable", "baseinfo_wqtmp_db.t_user_bianke")
      .option("user", "root")
      .option("password", "root")
      .load()

    val biankeNewUsers = biankeUserDataFrame.rdd.filter(row => {
      var userName = row.getAs[String](1)
      var bool = if (userRDD.contains(userName)) false else true
      bool
    }).map(row => {
      var userName = row.getAs[String](1)
      users(0, userName, "", "", 0, 0, "", "", "", "", "", "", "", "")
    }).toDF()
    *
     */



    userList
      //.union(biankeNewUsers)
      //.union(newUsers)
      .repartition(1)
      .write
      .format("csv")
      //.option("escape","")
      .option("sep",SEP)
      //.option("header",true)
      .mode(SaveMode.Overwrite)
      .save("E:/test/recommend-system/journal/UserPortraitOutputTemp")
    //print(biankeNewUsers.collect().length)
    //println("当前日志已知的用户数："+logTempDataFrame.join(userList,userList("UserName") === logTempDataFrame("un")).collect().length)

    /**
    logTempDataFrame
      .join(proDataFrame,proDataFrame("PYKM") === logDataFrame("ri"))
      .select(
        proDataFrame("title"),
        proDataFrame("module_id"),
        proDataFrame("keywords"),
        logDataFrame("un"),
        logDataFrame("vt")
      ).show()
      *
      * */
    //println("浏览article的人数"+logTempDataFrame.collect().length+",总浏览人数"+logDataFrame.collect().length)
    //logTempDataFrame.groupBy("ro").sum().show()
    //logTempDataFrame.show()
    spark.stop()
  }
}
