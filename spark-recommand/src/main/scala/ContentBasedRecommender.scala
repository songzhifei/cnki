import org.apache.spark.sql.{SaveMode, SparkSession}
import CommonFuction._
import CommonObj._


/*
*  根据内容相关度进行推荐
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

object ContentBasedRecommender {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("recommand-system").getOrCreate

    //val userList: RDD[UserTemp] = spark.read.textFile(args(0)).rdd.map(formatUsers)
    //2.1 获取用户画像数据（格式化用户兴趣标签数据）
    val dataFrame = spark
      .read
      .option("sep", ";")
      //.option("header", true)
      .csv(args(0))
      .toDF("id","name","prefList","latest_log_time")

    val userList = dataFrame.rdd.map(row => {
      var pre = row.getAs[String]("prefList")
      var id = row.getAs[String]("id")
      var map = jsonPrefListtoMap(pre)
      var name = row.getAs[String]("name")
      UserTemp(id.toLong, name, pre, map, row.getAs[String]("latest_log_time"))
    })

/*
    //2.2 获取所有待推荐的新闻列表（格式化所有新闻对应的关键词及关键词的权重）
    val newsList = spark.read.textFile(args(1)).rdd.map(formatNews).collect()

    val newsBroadCast = spark.sparkContext.broadcast(newsList)
    import spark.implicits._
    val newdataFrame = userList.map(user => formatRecommandTuple(user,newsBroadCast)).flatMap(_.toList).map(tuple => {
      recommendations(tuple._1, tuple._2,tuple._3,tuple._4,tuple._5)
    }).toDF()

    //dataFrame.show()

    newdataFrame.write.format("csv").mode(SaveMode.Overwrite).save(args(2))
    */
    spark.stop()

  }
}
