
import java.io.{FileInputStream, IOException}
import java.util.Properties

import CommonFuction._
import CommonObj.{UserArticleTempNew, UserConcernedSubjectTemp, UserTemp, UserTempNew}
import org.apache.spark.sql.{SaveMode, SparkSession}

object UserMain {
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

    println("-------------------当前处理的的用户画像地址："+args(0)+",日志地址："+args(1)+",生成用户画像存储位置："+args(2)+"--------------------")

    //1获取各个维度的画像
    //1.1. 获取用户画像数据（格式化用户兴趣标签数据）getYesterday() +
    var SingleArticleInterestFrame = spark.read.textFile(args(0)).rdd.map(line=>formatUsersNew(line,logTime)).filter(user=> !user.UserName.isEmpty).toDF()

    SingleArticleInterestFrame.show(20)

    var UserTotalRelatedLabelFrame = spark.read.textFile(args(1)).rdd.map(line=>formatRelatedLabel(line,logTime)).filter(user=> !user.UserName.isEmpty).toDF()

    UserTotalRelatedLabelFrame.show(20)

    //合并所有维度的画像
    var resultDataFrame = SingleArticleInterestFrame
      .join(UserTotalRelatedLabelFrame, Seq("UserName"))
      .select(
        SingleArticleInterestFrame("UserID")
        , SingleArticleInterestFrame("UserName")
        , SingleArticleInterestFrame("LawOfworkAndRest")
        , SingleArticleInterestFrame("Area")
        , SingleArticleInterestFrame("Age")
        , SingleArticleInterestFrame("Gender")
        , SingleArticleInterestFrame("ConcenedSubject")
        , SingleArticleInterestFrame("SingleArticleTotalInterest")
        , SingleArticleInterestFrame("SingleArticleRecentInterest")
        , UserTotalRelatedLabelFrame("TotalRelatedAuthor")
        , UserTotalRelatedLabelFrame("RecentRelatedAuthor")
        , SingleArticleInterestFrame("BooksInterests")
        , SingleArticleInterestFrame("JournalsInterests")
        , SingleArticleInterestFrame("ReferenceBookInterests")
        , SingleArticleInterestFrame("CustomerPurchasingPowerInterests")
        , SingleArticleInterestFrame("latest_log_time")
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
      .save(args(2))

    var hiveStatus = if(args.length >= 4) args(3).toInt else 0

    if(hiveStatus>0){
      println("----------------------用户画像正在保存至hive......--------------------------")

      try{
        //var sqlText = "create table user_main_old (UserID bigint, UserName string, LawOfworkAndRest string, Area string, Age TINYINT, Gender TINYINT, SingleArticleTotalInterest string,SingleArticleRecentInterest string, BooksInterests string, JournalsInterests string, ReferenceBookInterests string, CustomerPurchasingPowerInterests string, ProductviscosityInterests string, PurchaseIntentionInterests string, latest_log_time string) row format delimited fields terminated by '&' location '/cnki/Userportrait/%s/'".format(if(args.length>=6) args(5) else getToday())
        var sqlText = "ALTER TABLE user_main SET LOCATION '/cnki/UserMain/%s/'".format(if(args.length>=5) args(4) else getToday())

        import spark.sql

        //sql("DROP TABLE IF EXISTS user_portrait")

        sql(sqlText)
      }catch {
        case e: IOException =>
          e.printStackTrace()
      }
    }

    var mysqlStatus = if(args.length >= 6) args(5).toInt else 0

    if(mysqlStatus>0){
      println("----------------------用户画像正在保存至mysql......--------------------------")
      resultDataFrame.write
        .format("jdbc")
        //.option("url", "jdbc:mysql://master01:3306")
        //.option("dbtable", "cnki.user_main")
        .option("url", MysqlURL)
        .option("dbtable", DataTable)
        .option("user", UserName)
        .option("password", Password)
        .mode(SaveMode.Overwrite)
        .save()
    }

    spark.stop()
  }
}
