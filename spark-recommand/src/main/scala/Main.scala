import common._
import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.collection.mutable.ArrayBuffer

/*
* 1.更新用户画像\
*   1.1. 获取用户画像数据（格式化用户兴趣标签数据）
*   1.2. 获取用户浏览数据
*   1.3. 根据用户画像和浏览历史更新各个用户的用户兴趣标签
*   1.4. 用户新的画像数据保存到对应的表中
* 2.使用新画像根据CB生成推荐
*   2.1 获取用户画像数据（格式化用户兴趣标签数据）
*   2.2 获取所有待推荐的新闻列表（格式化所有新闻对应的关键词及关键词的权重）
*   2.3 循环各用户，计算所有新闻跟用户的相关度
*   2.4 过滤（相似度为0，已经看过的，重复的，已经推荐过，截取固定数量的的新闻）
*   2.5 生成推荐列表
* */

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("recommand-system").getOrCreate

    //1.1. 获取用户画像数据（格式化用户兴趣标签数据）
    val userList = spark.read.textFile(args(0)).rdd.map(formatUsers)

    //用户兴趣标签值衰减
    val userExtend = userList.map(autoDecRefresh)

    //1.2. 获取用户浏览数据
    val newsLogList = spark.read.textFile(args(1)).rdd.map(formatUserViewLogs).groupBy(_.username).map(row => {
      val iterator = row._2.iterator
      var arr = new ArrayBuffer[NewsLog_Temp]()
      while (iterator.hasNext) {
        arr += iterator.next()
      }
      (row._1, arr.toArray)
    })

    val newsLogBroadCast = spark.sparkContext.broadcast(newsLogList.collectAsMap())

    val logTime = spark.sparkContext.broadcast(getNowStr())

    val userRDD = userExtend.map(user => getUserPortrait(user,newsLogBroadCast,logTime))

    import spark.implicits._
    val userDataFrame = userRDD.map(user=>{
      users(user.id.toInt,user.username,"","",0,0,"","",user.prefListExtend.toString,"","","","",logTime.value)
    }).toDF()

    //更新用户画像
    /**/
    println("----------------------用户画像正在保存......--------------------------")
    userDataFrame.write
      .format("jdbc")
      .option("url", "jdbc:mysql://master02:3306")
      .option("dbtable", "centerDB.users_temp")
      .option("user", "root")
      .option("password", "root")
      .mode(SaveMode.Overwrite)
      .save()

    userDataFrame.repartition(1)
      .write
      .format("csv")
      //.option("escape","")
      .option("sep",";")
      //.option("header",true)
      .mode(SaveMode.Overwrite)
      .save(args(2))
    println("----------------------用户画像更新成功--------------------------")
    //生成推荐结果

    println("----------------------用户推荐结果正在更新......--------------------------")
    //2.2 获取所有待推荐的新闻列表（格式化所有新闻对应的关键词及关键词的权重）
    val newsList = spark.read.textFile(args(3)).rdd.map(formatNews).collect()

    val newsBroadCast = spark.sparkContext.broadcast(newsList)

    val recommandRDD = recommand(userRDD,newsBroadCast)

    val recommandDataFrame = recommandRDD.toDF()

    recommandDataFrame.write.format("csv").mode(SaveMode.Overwrite).save(args(4))

    println("----------------------用户推荐结果更新成功--------------------------")

    spark.stop()

  }
}
