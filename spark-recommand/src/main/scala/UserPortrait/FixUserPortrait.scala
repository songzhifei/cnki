import CommonFuction._
import CommonObj.usersNew
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
* 修复用户画像中作者由于中文逗号导致未分割完全的bug
* */
object FixUserPortrait {
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
    println("-------------------当前处理的的用户画像地址："+args(0)+"--------------------")
    //1.1. 获取用户画像数据（格式化用户兴趣标签数据）getYesterday() +
    var userList = spark.read.textFile(args(0)).rdd.map(line=>formatUsersNew(line,logTime)).filter(user=> !user.UserName.isEmpty)

    var userDataFrame = userList.map(user=>{
      usersNew(
        user.UserName
        ,""
        ,""
        ,0
        ,0
        ,user.ConcenedSubject
        ,user.SubConcenedSubject
        ,user.SingleArticleTotalInterest
        ,user.SingleArticleRecentInterest
        ,fixBug(jsonConcernedSubjectListToMap(user.TotalRelatedAuthor)).toString
        ,fixBug(jsonConcernedSubjectListToMap(user.RecentRelatedAuthor)).toString
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
    //更新用户画像
    resultDataFrame.show(10)

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
  def fixBug(old:CustomizedHashMap[String, CustomizedKeyWord]):CustomizedHashMap[String, CustomizedKeyWord]={
    var newMap = new CustomizedHashMap[String, CustomizedKeyWord]
    if(old.size() >0){
      val value = old.keySet().iterator()
      while(value.hasNext){
        val keyword = value.next()
        val word = old.get(keyword)
        if(keyword.contains("，")){
          val strings = keyword.split("，")
          for(key<-strings){
            newMap.put(key,word)
          }
        }else{
          newMap.put(keyword,word)
        }
      }
    }

    newMap
  }
}
