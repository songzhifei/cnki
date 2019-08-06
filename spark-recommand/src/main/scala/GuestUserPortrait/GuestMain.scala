//package GuestUserPortrait

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.collection.mutable.ArrayBuffer
import CommonFuction._
import CommonObj._

object GuestMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      //.master("local[*]")
      .appName("GuestMain")
      .getOrCreate
    import spark.implicits._
    val logTime = spark.sparkContext.broadcast(getNowStr())
    //1.1. 获取用户画像数据（格式化用户兴趣标签数据）getYesterday() +
    var userList = spark.read.textFile(args(0)).rdd.map(line=>formatUsers(line,logTime)).filter(user=> !user.UserName.isEmpty)

    //此处需要添加对一些基本的不符合条件的日志过滤掉
    val newsLogDataFrame = spark.read.textFile(args(1)).rdd.map(formatUserLog).filter(filterLog).toDF().where("un = '' and rcc !=''")

    //1.2通过日志更新新的用户信息
    val newUserList = getNewGuestUserList(userList.toDF(),newsLogDataFrame,logTime)

    userList = userList.union(newUserList)

    var userDataFrame = userList.toDF()


    //2.2 获取所有不同类别的兴趣标签
    var SingleArticleInterestList = userList.map(user=>{
      UserArticleTemp(user.UserID.toLong,user.UserName,user.SingleArticleInterest,jsonArticlePrefListtoMap(user.SingleArticleInterest),user.latest_log_time)
    })
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

    //用户兴趣标签值衰减
    val SingleArticleInterestExtend = SingleArticleInterestList.map(autoDecRefreshArticleInterests)
    //val BooksInterestsExtend = BooksInterestsList.map(autoDecRefresh)
    val JournalsInterestsExtend = JournalsInterestsList.map(autoDecRefresh)
    val ReferenceBookInterestsExtend = ReferenceBookInterestsList.map(autoDecRefresh)
    val CustomerPurchasingPowerInterestsExtend = CustomerPurchasingPowerInterestsList.map(autoDecRefresh)
    val ProductviscosityInterestsExtend = ProductviscosityInterestsList.map(autoDecRefresh)
    val PurchaseIntentionInterestsExtend = PurchaseIntentionInterestsList.map(autoDecRefresh)


    //根据用户浏览日志信息，更新用户文章类别画像
    val articleLog = newsLogDataFrame.where("ro = 'article'")

    val articleLogList = articleLog
      .rdd
      .map(row => {Log_Temp(row.getAs("gki"), row.getAs("vt"), "", "", row.getAs("rcc"),"", null)})
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

    val SingleArticleInterestFrame = SingleArticleInterestExtend.map(user=>getUserArticlePortrait(user,articleLogBroadCast)).toDF()

    //根据用户浏览日志信息，更新用户文章类别画像
    val bookLog = newsLogDataFrame.where("ro = 'tushu'").select("gki","ri")

    var BooksInterestsFrame:DataFrame = null;

    if(bookLog.count() > 0){

      val bookLogDataFrame = bookLog.rdd.map(row => {
        var id = row.getAs[String]("ri")
        var username = row.getAs[String]("gki")
        BookLogInfo(username,id, "", "")
      }).toDF()


      BooksInterestsFrame.show()

    }

    var resultDataFrame = userDataFrame
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
      .where("SingleArticleInterest !='{}'")

    resultDataFrame.show()
    //更新用户画像

    println("----------------------用户画像正在保存......--------------------------")

    resultDataFrame.repartition(1)
      .write
      .format("csv")
      //.option("escape","")
      .option("sep",SEP)
      //.option("header",true)
      .mode(SaveMode.Overwrite)
      .save(args(2))
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
