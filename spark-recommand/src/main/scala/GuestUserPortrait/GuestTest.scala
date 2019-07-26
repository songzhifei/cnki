import CommonFuction._
import CommonObj._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer
//package GuestUserPortrait

object GuestTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      //.master("local[*]")
      .appName("GuestTest")
      .getOrCreate
    import spark.implicits._
    val logTime = spark.sparkContext.broadcast(getNowStr())
    //1.1. 获取用户画像数据（格式化用户兴趣标签数据）getYesterday() +
    var userList = spark.read.textFile(args(0)).rdd.map(line=>formatUsers(line,logTime)).filter(user=> !user.UserName.isEmpty)

    //此处需要添加对一些基本的不符合条件的日志过滤掉
    //val newsLogDataFrame = spark.read.textFile(args(1)).rdd.map(formatUserLog).filter(filterLog).toDF().where("un = '' and rcc !=''")
    val newsLogRDD = spark.read.textFile(args(1)).rdd.map(formatUserLog).filter(filterLog)

    newsLogRDD.cache()
    //过滤浏览次数2000以上的人（正常认为浏览不应该超过2000次）
    //val uaArray = newsLogRDD.toDF().groupBy("ua").agg(Map("ua"->"count")).where("count(ua)>2000").rdd.map(row=>{row.getAs[String]("ua")}).collect()

    val uaTuples = newsLogRDD.toDF().groupBy("ua", "ci").agg(Map("ua" -> "count")).where("count(ua)>100").rdd.map(row => {
      (row.getAs[String]("ua"), row.getAs[String]("ci"))
    }).collect()
    val newsLogDataFrame = newsLogRDD.filter(row=>{
      var res = true
      import scala.util.control.Breaks._
      breakable{
        for(tuple <- uaTuples){
          res = !(tuple._1.equals(row.ua) && tuple._2.equals(row.ci))
          if(!res) break
        }
      }
      res
    }).toDF().where("un = '' and rcc !=''")

    //val newsLogDataFrame = newsLogRDD.filter(row=> !uaArray.contains(row.ua)).toDF().where("un = '' and rcc !=''")

    //1.2通过日志更新新的用户信息
    val newUserList = getNewGuestUserList(userList.toDF(),newsLogDataFrame,logTime)

    userList = userList.union(newUserList)

    var userDataFrame = userList.toDF()

    //println("newUserList:"+newUserList.collect().size+"userDataFrame"+userDataFrame.count())


    /**      *
      */

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
      .map(row => {
        var userName = row.getAs[String]("gki")
        var module_Str = row.getAs[String]("rcc")
        var newMap:CustomizedHashMap[String, Double] = new CustomizedHashMap[String, Double]
        val module_arr = module_Str.split(";")
        for(module_id <- module_arr){
          var value = 1D
          if(newMap.containsKey(module_id)){
            //value = 1D + newMap.get(module_id)
            newMap.remove(module_id)
          }
          newMap.put(module_id,value)
        }
        UserArticleTemp(0,userName,newMap.toString,newMap,"")
      })

    //println("-----------------------------------------------articleLogList:"+articleLogList.collect().length)

    var SingleArticleInterestFrame = SingleArticleInterestExtend.union(articleLogList).groupBy(_.UserName).map(row=>{
      var userName = row._1
      var newMap:CustomizedHashMap[String, Double] = new CustomizedHashMap[String, Double]
      val iterator = row._2.iterator
      while (iterator.hasNext){
        val userArticleTemp = iterator.next()

        val keyIterator = userArticleTemp.prefListExtend.keySet().iterator()

        while (keyIterator.hasNext){
          var key = keyIterator.next()
          var value = userArticleTemp.prefListExtend.get(key)
          if(newMap.containsKey(key)){
            value += newMap.get(key)
            newMap.remove(key)
          }
          newMap.put(key,value)
        }
      }
      users(0,userName,"","",0,0,newMap.toString,"","","","","","","")
    }).toDF()
    //println("------------------------------userDataFrame:"+userDataFrame.count()+"-----------------------------SingleArticleInterestFrame:"+SingleArticleInterestFrame.count())
    //articleLogList.show(20)
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

    //println("resultDataFrame:"+resultDataFrame.count())

    resultDataFrame.repartition(1)
      .write
      .format("csv")
      //.option("escape","")
      .option("sep",SEP)
      //.option("header",true)
      .mode(SaveMode.Overwrite)
      .save(args(2))


    spark.stop()

  }
}
