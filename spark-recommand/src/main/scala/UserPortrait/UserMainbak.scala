import java.io.{FileInputStream, IOException}
import CommonFuction._
import CommonObj.{UserArticleTempNew, UserConcernedSubjectTemp, UserTemp, UserTempNew}
import org.apache.spark.sql.{SaveMode, SparkSession}

object UserMainbak {
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
    //1.1. 获取用户画像数据（格式化用户兴趣标签数据）
    val userDataFrameOld = spark.read.parquet(args(0)).where("UserName != ''")
    //此处需要添加对一些基本的不符合条件的日志过滤掉
    val newsLogDataFrame = spark.read.parquet(args(1)).where("un is not null and un != ''")

    newsLogDataFrame.cache()

    //1.2通过日志更新新的用户信息
    val newUserList = getNewUserListNew(userDataFrameOld,newsLogDataFrame,logTime)


    var userDataFrame = userDataFrameOld.union(newUserList.toDF())

    userDataFrame.cache()
    //2.2 获取所有不同类别的兴趣标签
    var SingleArticleTotalInterestList = userDataFrame.rdd.map(row=>{
      UserArticleTempNew(0,row.getAs[String]("UserName"),"",jsonPrefListtoMapNew(row.getAs[String]("SingleArticleTotalInterest")),logTime.value)
    })
    //获取近期关注的标签
    var SingleArticleRecentInterestList = userDataFrame.rdd.map(row=>{
      UserArticleTempNew(0,row.getAs[String]("UserName"),"",autoDecRefreshArticleRecentInterests(jsonPrefListtoMapNew(row.getAs[String]("SingleArticleRecentInterest"))),logTime.value)
    })
    //用户关注的学科标签
    var ConcernedSubjectList = userDataFrame.rdd.map(row=>{
      UserConcernedSubjectTemp(0,row.getAs[String]("UserName"),"",jsonConcernedSubjectListToMap(row.getAs[String]("ConcenedSubject")),logTime.value)
    })
    //用户关注的子学科标签
    var SubConcernedSubjectList = userDataFrame.rdd.map(row=>{
      UserConcernedSubjectTemp(0,row.getAs[String]("UserName"),"",jsonConcernedSubjectListToMap(row.getAs[String]("SubConcenedSubject")),logTime.value)
    })
    /**/
    //相关作者累计标签
    var TotalRelatedList = userDataFrame.rdd.map(row=>{
      UserConcernedSubjectTemp(0,row.getAs[String]("UserName"),"",jsonConcernedSubjectListToMap(row.getAs[String]("TotalRelatedAuthor")),logTime.value)
    })
    //相关作者近期标签
    var RecentRelatedList = userDataFrame.rdd.map(row=>{
      UserConcernedSubjectTemp(0,row.getAs[String]("UserName"),"",autoDecRefreshAuthorRecentInterests(jsonConcernedSubjectListToMap(row.getAs[String]("RecentRelatedAuthor"))),logTime.value)
    })
    //搜索关键词累计标签
    var SearchKeywordList = userDataFrame.rdd.map(row=>{
      UserConcernedSubjectTemp(0,row.getAs[String]("UserName"),"",jsonConcernedSubjectListToMap(row.getAs[String]("SearchKeyword")),logTime.value)
    })
    //获取浏览文章的日志信息
    val articleLog = newsLogDataFrame.where("ro = 'article' and rcc !='' and ac in ('browse','buy','read','collect','concern','comment')")
    //获取搜索相关日志
    val searchLog = newsLogDataFrame.where("ac = 'search'")

    //日志缓存以方便被多次调用
    articleLog.cache()
    //根据日志生成最新的兴趣标签
    val articleLogList = getArticleLogInterests(articleLog)
    //根据日志生成最新的用户关注的标签
    val userConcernedSubjectFromLog = getUserConceredSubjects(articleLog,false)
    //根据日志生成最新的用户关注的子标签
    val userSubConcernedSubjectFromLog = getUserConceredSubjects(articleLog,true)
    //根据日志生成最新的相关用户标签
    val userRelatedAuthorFromLog = getRelatedAuthorFromLog(articleLog)
    //根据日志生成用户最新的搜索关键词标签
    val userSearchKeyWordFromLog = getSearchKeyWordFromLog(searchLog)

    articleLogList.cache()

    var SingleArticleTotalInterestFrame = unionOriginAndNewArticleLogInterests(SingleArticleTotalInterestList,articleLogList,true,true).toDF()

    var SingleArticleRecentInterestFrame = unionOriginAndNewArticleLogInterests(SingleArticleRecentInterestList,articleLogList,false,true).toDF()

    var userConcernedSubjectFrame = unionOriginAndNewUserConceredSubject(ConcernedSubjectList,userConcernedSubjectFromLog,"UserConceredSubject",logTime).toDF()

    var userSubConcernedSubjectFrame = unionOriginAndNewUserConceredSubject(SubConcernedSubjectList,userSubConcernedSubjectFromLog,"UserSubConceredSubject",logTime).toDF()

    var userTotalRelatedAuthorFrame = unionOriginAndNewUserConceredSubject(TotalRelatedList,userRelatedAuthorFromLog,"TotalRelatedAuthor",logTime).toDF()

    var userRecentRelatedAuthorFrame = unionOriginAndNewUserConceredSubject(RecentRelatedList,userRelatedAuthorFromLog,"RecentRelatedAuthor",logTime).toDF()

    var userSearchKeyWordFrame = unionOriginAndNewUserConceredSubject(SearchKeywordList,userSearchKeyWordFromLog,"SearchKeyword",logTime).toDF()

    var resultDataFrame = userDataFrame
      .join(userConcernedSubjectFrame, Seq("UserName"))
      .join(userSubConcernedSubjectFrame, Seq("UserName"))
      .join(SingleArticleTotalInterestFrame, Seq("UserName"))
      .join(SingleArticleRecentInterestFrame, Seq("UserName"))
      .join(userTotalRelatedAuthorFrame, Seq("UserName"))
      .join(userRecentRelatedAuthorFrame, Seq("UserName"))
      .join(userSearchKeyWordFrame, Seq("UserName"))
      .select(
         userDataFrame("UserName")
        , userDataFrame("LawOfworkAndRest")
        , userDataFrame("Area")
        , userDataFrame("Age")
        , userDataFrame("Gender")
        , userConcernedSubjectFrame("ConcenedSubject")
        , userSubConcernedSubjectFrame("SubConcenedSubject")
        , SingleArticleTotalInterestFrame("SingleArticleTotalInterest")
        , SingleArticleRecentInterestFrame("SingleArticleRecentInterest")
        , userTotalRelatedAuthorFrame("TotalRelatedAuthor")
        , userRecentRelatedAuthorFrame("RecentRelatedAuthor")
        , userSearchKeyWordFrame("SearchKeyword")
        , userDataFrame("JournalsInterests")
        , userDataFrame("ReferenceBookInterests")
        , userDataFrame("CustomerPurchasingPowerInterests")
        , userConcernedSubjectFrame("latest_log_time")
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
      //.format("csv")
      //.option("escape","")
      .option("sep",SEP)
      //.option("header",true)
      .mode(SaveMode.Overwrite)
      .parquet(args(2))
      //.save(args(2))

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
