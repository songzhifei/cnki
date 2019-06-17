import CommonFuction.{filterLog, formatUserLog, getYesterday}
import org.apache.spark.sql.{SaveMode, SparkSession}

object tushuRanking {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      //.master("local[*]")
      .appName("tushuRanking")
      .getOrCreate

    var num = if(args.length >=3) args(2).toInt else 100

    import spark.implicits._
    //此处需要添加对一些基本的不符合条件的日志过滤掉
    val newsLogDataFrame = spark.read.textFile(args(0)).rdd.map(formatUserLog).filter(filterLog).toDF()

    val tushuLog = newsLogDataFrame.where("ro = 'tushu'")

    val rankingArticle = tushuLog.rdd.groupBy(_.getAs[String]("ri")).map(tuple => {
      (tuple._1, tuple._2.toList.length)
    }).top(num)(Ordering.by(e => e._2))

    /*
        val rankingArticle = articleLog.rdd.groupBy(_.getAs[String]("ri")).map(tuple => {
      (tuple._1, tuple._2.toList.length)
    }).sortBy(_._2, ascending = false).take(100)
    * */

    val dataFrame = spark.sparkContext.parallelize(rankingArticle).map(tuple => {
      tushuRankingObj(tuple._1, tuple._2)
    }).toDF()
    dataFrame.repartition(1)
      .write
      .format("csv")
      //.option("escape","")
      .option("sep",",")
      //.option("header",true)
      .mode(SaveMode.Overwrite)
      .save(args(1))

    println(getYesterday()+":图书浏览排行榜前100篇保存成功！")

    //rankingArticle.foreach{println}

    spark.stop()

  }
  case class tushuRankingObj(id:String,num:Int)
}
