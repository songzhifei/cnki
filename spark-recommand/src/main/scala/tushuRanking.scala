import CommonFuction.{filterLog, formatUserLog, getYesterday}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 图书排行榜生成app
  */
object tushuRanking {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      //.master("local[*]")
      .appName("tushuRanking")
      .getOrCreate

    var num = if(args.length >=3) args(2).toInt else 100

    //import spark.implicits._
    //此处需要添加对一些基本的不符合条件的日志过滤掉
    //val newsLogDataFrame = spark.read.textFile(args(0)).rdd.map(formatUserLog).filter(filterLog).toDF()
    val newsLogDataFrame = spark.read.parquet(args(0))

    val tushuLog = newsLogDataFrame.where("ro = 'tushu'")

    val articleNewFrame = tushuLog.groupBy("ri").agg(Map("ri"->"count"))

    val dataFrame = articleNewFrame.orderBy(articleNewFrame("count(ri)").desc).limit(num)

    /*
        val rankingArticle = tushuLog.rdd.groupBy(_.getAs[String]("ri")).map(tuple => {
      (tuple._1, tuple._2.toList.length)
    }).top(num)(Ordering.by(e => e._2))
        val rankingArticle = articleLog.rdd.groupBy(_.getAs[String]("ri")).map(tuple => {
      (tuple._1, tuple._2.toList.length)
    }).sortBy(_._2, ascending = false).take(100)

        val dataFrame = spark.sparkContext.parallelize(rankingArticle).map(tuple => {
      tushuRankingObj(tuple._1, tuple._2)
    }).toDF()
    * */


    dataFrame
      .coalesce(1)
      .write
      .format("csv")
      .option("sep",",")
      .mode(SaveMode.Overwrite)
      .save(args(1))

    println(getYesterday()+":图书浏览排行榜前100篇保存成功！")

    //rankingArticle.foreach{println}

    spark.stop()

  }
  case class tushuRankingObj(id:String,num:Int)
}
