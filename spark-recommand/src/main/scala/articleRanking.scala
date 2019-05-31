import common.{SEP, filterLog, formatUserLog}
import org.apache.spark.sql.{SaveMode, SparkSession}

object articleRanking {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("testFormatUserLog").getOrCreate

    import spark.implicits._
    //此处需要添加对一些基本的不符合条件的日志过滤掉
    val newsLogDataFrame = spark.read.textFile("E:/test/recommend-system/journal/userlog/").rdd.map(formatUserLog).filter(filterLog).toDF()

    val articleLog = newsLogDataFrame.where("ro = 'article'")

    val rankingArticle = articleLog.rdd.groupBy(_.getAs[String]("ri")).map(tuple => {
      (tuple._1, tuple._2.toList.length)
    }).sortBy(_._2, ascending = false).take(100)

    /*
    articleLog.rdd.groupBy(_.getAs[String]("ri")).map(tuple => {
      (tuple._1, tuple._2.toList.length)
    }).top(100)(Ordering.by(e => e._2))
    * */

    val dataFrame = spark.sparkContext.parallelize(rankingArticle).map(tuple => {
      articleRankingObj(tuple._1, tuple._2)
    }).toDF()
    dataFrame.repartition(1)
      .write
      .format("csv")
      //.option("escape","")
      .option("sep",",")
      //.option("header",true)
      .mode(SaveMode.Overwrite)
      .save("E:/test/recommend-system/articleRanking")

    println("文章浏览排行榜前100篇保存成功！")

    //rankingArticle.foreach{println}

    spark.stop()

  }
  case class articleRankingObj(id:String,num:Int)
}
