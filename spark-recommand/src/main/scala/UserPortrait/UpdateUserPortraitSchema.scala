import java.io.IOException

import CommonFuction._
import CommonObj.{UserArticleTempNew, UserConcernedSubjectTemp}
import org.apache.spark.sql.{SaveMode, SparkSession}

object UpdateUserPortraitSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("user-main")
      .config("hive.metastore.uris","thrift://master01:9083")//SPARK读取hive中的元数据必须配置metastore地址
      .config("spark.sql.warehouse.dir", "hdfs://nameservice1/user/hive/warehouse")//配置hive的数据存储目录
      .enableHiveSupport()
      .getOrCreate

    //加载外部配置文件
    getProperty()

    import spark.implicits._
    val logTime = spark.sparkContext.broadcast(getNowStr())
    //1.1. 获取用户画像数据（格式化用户兴趣标签数据）getYesterday() +
    var userList = spark.read.textFile(args(0)).rdd.map(line=>formatUsersNew(line,logTime)).filter(user=> !user.UserName.isEmpty)

    var userDataFrame = userList.toDF()

    var resultDataFrame = userDataFrame
      .select(
         userDataFrame("UserName")
        , userDataFrame("LawOfworkAndRest")
        , userDataFrame("Area")
        , userDataFrame("Age")
        , userDataFrame("Gender")
        , userDataFrame("ConcenedSubject")
        , userDataFrame("SubConcenedSubject")
        , userDataFrame("SingleArticleTotalInterest")
        , userDataFrame("SingleArticleRecentInterest")
        , userDataFrame("TotalRelatedAuthor")
        , userDataFrame("RecentRelatedAuthor")
        , userDataFrame("BooksInterests")
        , userDataFrame("JournalsInterests")
        , userDataFrame("ReferenceBookInterests")
        , userDataFrame("CustomerPurchasingPowerInterests")
        , userDataFrame("latest_log_time")
      )
    //更新用户画像
    resultDataFrame.show(10)
    /*
    *
     */
    println("----------------------用户画像正在保存......--------------------------")

    resultDataFrame
      //.repartition(1)
      .write
      .format("csv")
      //.option("escape","")
      .option("sep",SEP)
      //.option("header",true)
      .mode(SaveMode.Overwrite)
      .save(args(1))

    spark.stop()
  }
}
