import java.io.IOException

import CommonFuction._
import CommonObj._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

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
    val spark = SparkSession
      .builder
      //.master("local[*]")
      .config("hive.metastore.uris","thrift://master01:9083")//SPARK读取hive中的元数据必须配置metastore地址
      .config("spark.sql.warehouse.dir", "hdfs://nameservice1/user/hive/warehouse")//配置hive的数据存储目录
      .appName("recommand-system")
      .enableHiveSupport()
      .getOrCreate
    import spark.implicits._
    val logTime = spark.sparkContext.broadcast(getNowStr())
    //1.1. 获取用户画像数据（格式化用户兴趣标签数据）getYesterday() +
    var userList = spark.read.textFile(args(0)).rdd.map(line=>formatUsers(line,logTime)).filter(user=> !user.UserName.isEmpty)

    //此处需要添加对一些基本的不符合条件的日志过滤掉
    val newsLogDataFrame = spark.read.textFile(args(1)).rdd.map(formatUserLog).filter(filterLog).toDF().where("un is not null and un != '' and ac = 'browse' and rcc !=''")

    newsLogDataFrame.cache()

    //1.2通过日志更新新的用户信息
    val newUserList = getNewUserList(userList.toDF(),newsLogDataFrame,logTime)

    userList = userList.union(newUserList)

    var userDataFrame = userList.toDF()

    //2.2 获取所有不同类别的兴趣标签
    var SingleArticleInterestList = userList.map(user=>{
      UserTemp(user.UserID.toLong,user.UserName,user.SingleArticleInterest,jsonPrefListtoMap(user.SingleArticleInterest),user.latest_log_time)
    })
    /*
    var BooksInterestsList = userList.map(user=>{
      UserTemp(user.UserID.toLong,user.UserName,user.BooksInterests,jsonPrefListtoMap(user.BooksInterests),user.latest_log_time)
    })
    var JournalsInterestsList = userList.map(user=>{
      UserTemp(user.UserID.toLong,user.UserName,user.JournalsInterests,jsonPrefListtoMap(user.JournalsInterests),user.latest_log_time)
    })
    var ReferenceBookInterestsList = userList.map(user=>{
      UserTemp(user.UserID.toLong,user.UserName,user.ReferenceBookInterests,jsonPrefListtoMap(user.ReferenceBookInterests),user.latest_log_time)
    })
    var CustomerPurchasingPowerInterestsList = userList.map(user=>{
      UserTemp(user.UserID.toLong,user.UserName,user.CustomerPurchasingPowerInterests,jsonPrefListtoMap(user.CustomerPurchasingPowerInterests),user.latest_log_time)
    })
    var ProductviscosityInterestsList = userList.map(user=>{
      UserTemp(user.UserID.toLong,user.UserName,user.ProductviscosityInterests,jsonPrefListtoMap(user.ProductviscosityInterests),user.latest_log_time)
    })
    var PurchaseIntentionInterestsList = userList.map(user=>{
      UserTemp(user.UserID.toLong,user.UserName,user.PurchaseIntentionInterests,jsonPrefListtoMap(user.PurchaseIntentionInterests),user.latest_log_time)
    })
*/
    //用户兴趣标签值衰减
    val SingleArticleInterestExtend = SingleArticleInterestList.map(autoDecRefresh)
    //val BooksInterestsExtend = BooksInterestsList.map(autoDecRefresh)
    //val JournalsInterestsExtend = JournalsInterestsList.map(autoDecRefresh)
    //val ReferenceBookInterestsExtend = ReferenceBookInterestsList.map(autoDecRefresh)
    //val CustomerPurchasingPowerInterestsExtend = CustomerPurchasingPowerInterestsList.map(autoDecRefresh)
    //val ProductviscosityInterestsExtend = ProductviscosityInterestsList.map(autoDecRefresh)
    //val PurchaseIntentionInterestsExtend = PurchaseIntentionInterestsList.map(autoDecRefresh)


    //根据用户浏览日志信息，更新用户文章类别画像
    val articleLog = newsLogDataFrame.where("ro = 'article'")

    val articleLogList = articleLog
      .rdd
      .map(row => {Log_Temp(row.getAs("un"), row.getAs("vt"), "", "", row.getAs("rcc"),row.getAs("rkd"), null)})
      .groupBy(_.username)
      .map(row => {
        val iterator = row._2.iterator
        var arr = new ArrayBuffer[Log_Temp]()
        while (iterator.hasNext) {
          arr += iterator.next()
        }
        (row._1, arr.toArray)
      })



    val articleLogBroadCast = spark.sparkContext.broadcast(articleLogList.collectAsMap())

    val SingleArticleInterestFrame = SingleArticleInterestExtend.map(user=>getUserPortrait(user,articleLogBroadCast,logTime)).map(user=>{
      users(user.UserID,user.UserName,"","",0,0,user.prefListExtend.toString,"","","","","","",user.latest_log_time)
    }).toDF()

    //SingleArticleInterestFrame.show()

    //根据用户浏览日志信息，更新用户文章类别画像
    val bookLog = newsLogDataFrame.where("ro = 'tushu'").select("un","ri")

    var BooksInterestsFrame:DataFrame = null;

    if(bookLog.count() > 0){

      val bookLogDataFrame = bookLog.rdd.map(row => {
        var id = row.getAs[String]("ri")
        var username = row.getAs[String]("un")
        BookLogInfo(username,id, "", "")
      }).toDF()

      //bookLogDataFrame
      /*
      //1.2. 获取图书基本数据
      val bookBaseInfoDataFrame = spark.read.textFile(args(2)).rdd.map(formateBookInfo).filter(!_.id.isEmpty).toDF()

      val bookLogDataFrameTemp = bookLogDataFrame.join(bookBaseInfoDataFrame,Seq("id")).select(bookLogDataFrame("id"),bookLogDataFrame("username"),bookBaseInfoDataFrame("class_code"),bookBaseInfoDataFrame("keywords"))

      bookLogDataFrameTemp.show()

      var bookLogList = bookLogDataFrameTemp.rdd.map(row=>{
        var id = row.getAs("id")
        var username = row.getAs("username")
        var module_id = row.getAs("class_code")
        var preflist = jsonArticlePrefListtoMap(row.getAs("keywords"))
        Log_Temp(username,"","","",module_id,preflist)
      }).groupBy(_.username).map(row => {
        val iterator = row._2.iterator
        var arr = new ArrayBuffer[Log_Temp]()
        while (iterator.hasNext) {
          arr += iterator.next()
        }
        (row._1, arr.toArray)
      })

      val bookLogBroadCast = spark.sparkContext.broadcast(bookLogList.collectAsMap())

      val BooksRDD = BooksInterestsList.map(user => getUserPortrait(user,bookLogBroadCast,logTime))

      BooksInterestsFrame = BooksRDD.map(user=>{
        users(user.UserID,user.UserName,"","",0,0,"",user.prefListExtend.toString,"","","","","",user.latest_log_time)
      }).toDF()

      BooksInterestsFrame.show()

    }
**/
    }
    var resultDataFrame:DataFrame = null

    if(BooksInterestsFrame != null && BooksInterestsFrame.count()>0){
      resultDataFrame = userDataFrame
        .join(BooksInterestsFrame, Seq("UserID", "UserName"))
        .join(SingleArticleInterestFrame, Seq("UserID", "UserName"))
        .select(
          userDataFrame("UserID")
          , userDataFrame("UserName")
          , userDataFrame("LawOfworkAndRest")
          , userDataFrame("Area")
          , userDataFrame("Age")
          , userDataFrame("Gender")
          , SingleArticleInterestFrame("SingleArticleInterest")
          , BooksInterestsFrame("BooksInterests")
          , userDataFrame("JournalsInterests")
          , userDataFrame("ReferenceBookInterests")
          , userDataFrame("CustomerPurchasingPowerInterests")
          , userDataFrame("ProductviscosityInterests")
          , userDataFrame("PurchaseIntentionInterests")
          , userDataFrame("latest_log_time")
        )
    }else{
      resultDataFrame = userDataFrame
        //.join(JournalsInterestsFrame, Seq("UserID", "UserName"))
        .join(SingleArticleInterestFrame, Seq("UserID", "UserName"))
        .select(
          userDataFrame("UserID")
          , userDataFrame("UserName")
          , userDataFrame("LawOfworkAndRest")
          , userDataFrame("Area")
          , userDataFrame("Age")
          , userDataFrame("Gender")
          , SingleArticleInterestFrame("SingleArticleInterest")
          , userDataFrame("BooksInterests")
          , userDataFrame("JournalsInterests")
          , userDataFrame("ReferenceBookInterests")
          , userDataFrame("CustomerPurchasingPowerInterests")
          , userDataFrame("ProductviscosityInterests")
          , userDataFrame("PurchaseIntentionInterests")
          , userDataFrame("latest_log_time")
        )
    }

    //resultDataFrame.show()
    //更新用户画像

    println("----------------------用户画像正在保存......--------------------------")
    /** */

    var mysqlStatus = if(args.length >= 4) args(3).toInt else 0

    if(mysqlStatus>0){
      println("----------------------用户画像正在保存至mysql......--------------------------")
      resultDataFrame.write
        .format("jdbc")
        .option("url", "jdbc:mysql://master01:3306")
        .option("dbtable", "cnki.user_portrait")
        .option("user", "root")
        .option("password", "root")
        .mode(SaveMode.Overwrite)
        .save()
    }

    resultDataFrame.repartition(1)
      .write
      .format("csv")
      //.option("escape","")
      .option("sep",SEP)
      //.option("header",true)
      .mode(SaveMode.Overwrite)
      .save(args(2))

    var hiveStatus = if(args.length >= 5) args(4).toInt else 0

    if(hiveStatus>0){
      println("----------------------用户画像正在保存至hive......--------------------------")

      try{
        //var sqlText = "create table user_portrait (UserID bigint, UserName string, LawOfworkAndRest string, Area string, Age TINYINT, Gender TINYINT, SingleArticleInterest string, BooksInterests string, JournalsInterests string, ReferenceBookInterests string, CustomerPurchasingPowerInterests string, ProductviscosityInterests string, PurchaseIntentionInterests string, latest_log_time string) row format delimited fields terminated by '&' location '/cnki/Userportrait/%s/'".format(if(args.length>=6) args(5) else getToday())
        var sqlText = "ALTER TABLE user_portrait SET LOCATION '/cnki/Userportrait/%s/'".format(if(args.length>=6) args(5) else getToday())

        import spark.sql

        //sql("DROP TABLE IF EXISTS user_portrait")

        sql(sqlText)
      }catch {
        case e: IOException =>
          e.printStackTrace()
      }
    }
    println("----------------------用户画像更新成功--------------------------")
    //生成推荐结果

    println("----------------------用户推荐结果正在更新......--------------------------")

    //val recommandRDD = recommand(userRDD,newsBroadCast)

    //val recommandDataFrame = recommandRDD.toDF()

    //recommandDataFrame.write.format("csv").mode(SaveMode.Overwrite).save(args(4))

    println("----------------------用户推荐结果更新成功--------------------------")

    spark.stop()


}
}
