import java.text.SimpleDateFormat
import java.util.Date

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

    val newsBroadCast = spark.sparkContext.broadcast(newsLogList.collectAsMap())

    val logTime = spark.sparkContext.broadcast(getNowStr())

    import spark.implicits._
    val dataFrame = userExtend.map(user => getUserPortrait(user,newsBroadCast,logTime)).toDF()

    //dataFrame.show()


    /*

    dataFrame.write
      .format("jdbc")
      .option("url", "jdbc:mysql://master02:3306")
      .option("dbtable", "centerDB.users_temp")
      .option("user", "root")
      .option("password", "root")
      .mode(SaveMode.Overwrite)
      .save()
    */
    dataFrame.repartition(1)
      .write
      .format("csv")
      //.option("escape","")
      .option("sep",";")
      //.option("header",true)
      .mode(SaveMode.Overwrite)
      .save(args(2))
    println("----------------------用户画像更新成功--------------------------")

    spark.stop()

  }
}
