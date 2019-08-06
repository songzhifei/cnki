
import java.io.IOException

import CommonFuction._
import CommonObj.{UserArticleTempNew, UserConcernedSubjectTemp, usersNew, usersToMysql}
import org.apache.spark.sql.{SaveMode, SparkSession}

object UserPortraitToMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("user-UserPortraitToMysql")
      .config("hive.metastore.uris","thrift://master01:9083")//SPARK读取hive中的元数据必须配置metastore地址
      .config("spark.sql.warehouse.dir", "hdfs://nameservice1/user/hive/warehouse")//配置hive的数据存储目录
      .enableHiveSupport()
      .getOrCreate

    //加载外部配置文件
    getProperty()

    import spark.implicits._
    val logTime = spark.sparkContext.broadcast(getNowStr())
    var size = if(args.length >=3) args(2).toInt else 3
    println("-------------------当前处理的的用户画像地址："+args(0)+",保存地址："+args(1)+"size大小："+size+"--------------------")
    //1.1. 获取用户画像数据（格式化用户兴趣标签数据）getYesterday() +
    val userDataFrameOld = spark.read.parquet(args(0)).where("UserName != ''")

    var userDataFrame = userDataFrameOld.rdd.map(row=>{
      var hasPortrait = 1
      val ConcenedSubject = getNewSubjectHashMap(jsonConcernedSubjectListToMap(row.getAs[String]("ConcenedSubject")),size)
      val SubConcenedSubject = getNewSubjectHashMap(jsonConcernedSubjectListToMap(row.getAs[String]("SubConcenedSubject")),size)
      val SingleArticleTotalInterest = getNewArticleHashMap(jsonPrefListtoMapNew(row.getAs[String]("SingleArticleTotalInterest")),size)
      val SingleArticleRecentInterest = getNewArticleHashMap(jsonPrefListtoMapNew(row.getAs[String]("SingleArticleRecentInterest")),size)
      val TotalRelatedAuthor = getNewSubjectHashMap(jsonConcernedSubjectListToMap(row.getAs[String]("TotalRelatedAuthor")),size)
      val RecentRelatedAuthor = getNewSubjectHashMap(jsonConcernedSubjectListToMap(row.getAs[String]("RecentRelatedAuthor")),size)
      val SearchKeyword = getNewSubjectHashMap(jsonConcernedSubjectListToMap(row.getAs[String]("SearchKeyword")),size)
      if(ConcenedSubject.equals("{}") && SubConcenedSubject.equals("{}") && SingleArticleTotalInterest.equals("{}")  && SingleArticleRecentInterest.equals("{}") && TotalRelatedAuthor.equals("{}") && RecentRelatedAuthor.equals("{}") && SearchKeyword.equals("{}")) hasPortrait = 0

      usersToMysql(
        row.getAs("UserName")
        ,""
        ,""
        ,0
        ,0
        ,ConcenedSubject
        ,SubConcenedSubject
        ,SingleArticleTotalInterest
        ,SingleArticleRecentInterest
        ,TotalRelatedAuthor
        ,RecentRelatedAuthor
        ,SearchKeyword
        ,""
        ,""
        ,""
        ,hasPortrait
        ,row.getAs[String]("latest_log_time"))
    }).toDF()
/*
    import org.apache.spark.sql.functions._
    val getNewArticleHashMap1 = udf(getNewArticleHashMap _)

    val getNewSubjectHashMap1 = udf(getNewSubjectHashMap _)

    val jsonConcernedSubjectListToMap1 = udf(jsonConcernedSubjectListToMap _)

    val jsonPrefListtoMapNew1 = udf(jsonPrefListtoMapNew _)

    var userDataFrame = userDataFrameOld.select(
      col("UserName"),
      col("LawOfworkAndRest"),
      col("Area"),
      col("Age"),
      col("Gender"),
      col("UserName"),
      col("UserName"),
      getNewSubjectHashMap1(jsonConcernedSubjectListToMap1(col("ConcenedSubject"))) as "ConcenedSubject",
      getNewSubjectHashMap1(jsonConcernedSubjectListToMap1(col("SubConcenedSubject"))) as "SubConcenedSubject",
      getNewArticleHashMap1(jsonPrefListtoMapNew1(col("SingleArticleTotalInterest"))) as "SingleArticleTotalInterest",
      getNewArticleHashMap1(jsonPrefListtoMapNew1(col("SingleArticleRecentInterest"))) as "SingleArticleRecentInterest",
      getNewSubjectHashMap1(jsonConcernedSubjectListToMap1(col("TotalRelatedAuthor"))) as "TotalRelatedAuthor",
      getNewSubjectHashMap1(jsonConcernedSubjectListToMap1(col("RecentRelatedAuthor"))) as "RecentRelatedAuthor",
      col("BooksInterests"),
      col("JournalsInterests"),
      col("ReferenceBookInterests"),
      col("CustomerPurchasingPowerInterests"),
      col("latest_log_time")
    )

    var userDataFrame = userList.map(user=>{
      usersNew(
        user.UserName
        ,""
        ,""
        ,0
        ,0
        ,getNewSubjectHashMap(jsonConcernedSubjectListToMap(user.ConcenedSubject)).toString
        ,getNewSubjectHashMap(jsonConcernedSubjectListToMap(user.SubConcenedSubject)).toString
        ,getNewArticleHashMap(jsonPrefListtoMapNew(user.SingleArticleTotalInterest),size).toString
        ,getNewArticleHashMap(jsonPrefListtoMapNew(user.SingleArticleRecentInterest),size).toString
        ,getNewSubjectHashMap(jsonConcernedSubjectListToMap(user.TotalRelatedAuthor)).toString
        ,getNewSubjectHashMap(jsonConcernedSubjectListToMap(user.RecentRelatedAuthor)).toString
        ,""
        ,""
        ,""
        ,""
        ,user.latest_log_time)
    }).toDF()



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
      */
    //更新用户画像
    userDataFrame.show(10)

    println("----------------------用户画像正在保存......--------------------------")

    userDataFrame
      .write
      //.format("csv")
      .option("sep",SEP)
      .mode(SaveMode.Overwrite)
      .parquet(args(1))

    var hiveStatus = if(args.length >= 4) args(3).toInt else 0

    if(hiveStatus>0){
      println("----------------------用户画像正在保存至hive......--------------------------")

      try{
        var sqlText = "ALTER TABLE user_main_parquet SET LOCATION '/cnki/UserMain/mysql/%s/'".format(if(args.length>=5) args(4) else getToday())
        import spark.sql
        sql(sqlText)
      }catch {
        case e: IOException =>
          e.printStackTrace()
      }
    }
    spark.stop()
  }
  def getNewArticleHashMap(oldHashMap:CustomizedHashMap[String,CustomizedHashMap[String,CustomizedKeyWord]],size:Int):String={

    var newMap = new CustomizedHashMap[String,CustomizedHashMap[String,CustomizedKeyWord]]

    val articleIterator = oldHashMap.keySet().iterator()
    while(articleIterator.hasNext){
      val articleModuleID = articleIterator.next()
      if(newMap.size() < size){
        val stringToWord = oldHashMap.get(articleModuleID)
        var newStringToWord = new CustomizedHashMap[String,CustomizedKeyWord]
        if(stringToWord.size() <= size){
          newStringToWord = stringToWord
        }else{
          val iterator = stringToWord.keySet().iterator()
          while(iterator.hasNext){
            val keyword = iterator.next()
            if(newStringToWord.size() < size){
              newStringToWord.put(keyword,stringToWord.get(keyword))
            }
          }
        }
        newMap.put(articleModuleID,newStringToWord)
      }
    }
    newMap.toString
  }
  def getNewSubjectHashMap(oldHashMap:CustomizedHashMap[String,CustomizedKeyWord],size:Int):String={

    var newMap = new CustomizedHashMap[String,CustomizedKeyWord]

    val articleIterator = oldHashMap.keySet().iterator()
    while(articleIterator.hasNext){
      val articleModuleID = articleIterator.next()
      if(newMap.size() < size){
        newMap.put(articleModuleID,oldHashMap.get(articleModuleID))
      }
    }
    newMap.toString
  }
}
