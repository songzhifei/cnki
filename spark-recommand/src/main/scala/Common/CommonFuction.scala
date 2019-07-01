//package Common

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{ArrayList, Date}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.codehaus.jackson.JsonParseException
import org.codehaus.jackson.`type`.TypeReference
import org.codehaus.jackson.map.{JsonMappingException, ObjectMapper}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import scala.collection.mutable.ArrayBuffer
import CommonObj._
/**
  *
  * 公用方法类
  *
  * */
object CommonFuction {


  var objectMapper:ObjectMapper = null

  var SEP = "&"

  var spiderArray = Array(
    "spider",
    "+Galaxy+Nexus+Build/ICL53F)+",
    "bingbot",
    "trendkite-akashic-crawler",
    "SHV-E250S Build/JZO54K",
    "python-requests",
    "Googlebot-Image",
    "Scrapy",
    "Baiduspider",
    "YisouSpider",
    "Kazehakase",
    "(+https://scrapy.org)",
    "Windows; U; MSIE",
    "Bytespider",
    "ToutiaoSpider",
    "360Spider",
    "baiduboxapp",
    "bot",
    "SinaWeiboBot",
    "Googlebot",
    "DotBot",
    "Facebot",
    "applebot",
    "cliqzbot",
    "SurveyBot",
    "MagiBot",
    "oBot",
    "KomodiaBot",
    "aiHitBot",
    "LinkpadBot",
    "MJ12bot",
    "TurnitinBot",
    "YoudaoBot",
    "http://mappydata.net/bot",
    "coccocbot-web",
    "Twitterbot",
    "YandexMobileBot",
    "SEMrushBot"
  )

  def autoDecRefresh(user:UserTemp): UserTemp ={

    //用于删除喜好值过低的关键词
    val keywordToDelete = new ArrayList[String]

    val map =user.prefListExtend

    var baseAttenuationCoefficient = 0.9

    var times = 1L

    if(!user.latest_log_time.isEmpty && user.latest_log_time != "\"\"")
      times = intervalTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(user.latest_log_time),new Date())

    for(i<- 0 to times.toInt - 1) baseAttenuationCoefficient *= baseAttenuationCoefficient

    var newMap:CustomizedHashMap[String, CustomizedHashMap[String, Double]] = new CustomizedHashMap[String, CustomizedHashMap[String, Double]]

    val ite = map.keySet.iterator

    while (ite.hasNext){
      //用户对应模块的喜好不为空
      val moduleId = ite.next
      val moduleMap = map.get(moduleId)
      //N:{"X1":n1,"X2":n2,.....}
      if (moduleMap.toString != "{}") {
        val inIte = moduleMap.keySet.iterator
        while (inIte.hasNext) {
          val key = inIte.next
          //累计TFIDF值乘以衰减系数
          val result = moduleMap.get(key) * baseAttenuationCoefficient
          //if (result < 10) keywordToDelete.add(key)
          moduleMap.put(key, result)
        }
      }
      import scala.collection.JavaConversions._
      for (deleteKey <- keywordToDelete) {
        moduleMap.remove(deleteKey)
      }
      //newMap.put()
      keywordToDelete.clear()
      newMap.put(moduleId,moduleMap)
    }

    UserTemp(user.UserID,user.UserName,user.prefList,newMap,user.latest_log_time)

  }

  def autoDecRefreshArticleInterests(user:UserArticleTemp): UserArticleTemp ={

    //用于删除喜好值过低的关键词
    val keywordToDelete = new ArrayList[String]

    val map =user.prefListExtend

    var baseAttenuationCoefficient = 0.9

    var times = 1L

    if(!user.latest_log_time.isEmpty && user.latest_log_time != "\"\"" && user.latest_log_time !="{}")
      times = intervalTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(user.latest_log_time),new Date())

    for(i<- 0 to times.toInt - 1) baseAttenuationCoefficient *= baseAttenuationCoefficient

    var newMap:CustomizedHashMap[String, Double] = new CustomizedHashMap[String, Double]


    val ite = map.keySet.iterator

    while (ite.hasNext){
      //用户对应模块的喜好不为空
      val key = ite.next

      var value = map.get(key)

      value = value * baseAttenuationCoefficient

      if(value < 0.01) keywordToDelete.add(key)

      newMap.put(key,value)

      import scala.collection.JavaConversions._
      for (deleteKey <- keywordToDelete) {
        newMap.remove(deleteKey)
      }
      keywordToDelete.clear()
    }

    UserArticleTemp(user.UserID,user.UserName,user.prefList,newMap,user.latest_log_time)

  }

  def getUserPortrait(user:UserTemp,newsBroadCast:Broadcast[collection.Map[String, Array[Log_Temp]]],logTime:Broadcast[String]): UserTemp ={
    val newsList: Array[Log_Temp] = newsBroadCast.value.get(user.UserName).getOrElse(new Array[Log_Temp](0))
    println("处理前的rateMap：" + user.prefListExtend.toString)
    if (newsList.length >0) {
      //1.3. 根据用户画像和浏览历史更新各个用户的用户兴趣标签
      for(news <- newsList){
        var rateMap: CustomizedHashMap[String, Double] = user.prefListExtend.get(news.module_id)
        println("原始rateMap：" + rateMap,news.map)
        if(news.map !=null && news.map.size > 0 && rateMap == null){
          rateMap = new CustomizedHashMap[String,Double]()
          user.prefListExtend.put(news.module_id,rateMap)

          val keywordIte = news.map.keySet().iterator()
          while(keywordIte.hasNext){
            val name = keywordIte.next()
            val score = news.map.get(name)
            if(rateMap.containsKey(name))
              rateMap.put(name, rateMap.get(name) + score)
            else
              rateMap.put(name, score)
          }
        }

        println("处理完的rateMap：" + user.prefListExtend.get(news.module_id))
      }
    }
    println("处理后的rateMap：" + user.prefListExtend.toString)

    UserTemp(user.UserID,user.UserName,user.prefList,user.prefListExtend,logTime.value)
    //users(user.id.toInt,user.username,"","",0,0,"","",user.prefListExtend.toString,"","","","",logTime.value)
  }

  def getUserArticlePortrait(user:UserArticleTemp,newsBroadCast:Broadcast[collection.Map[String, Array[Log_Temp]]]):users={
    val newsList: Array[Log_Temp] = newsBroadCast.value.get(user.UserName).getOrElse(new Array[Log_Temp](0))

    newsList.groupBy(_.module_id).map(row=>{

      val keys = row._1.split(";")

      for(key<- keys){
        var value = 0D
        if(user.prefListExtend.containsKey(key)){
          value = user.prefListExtend.get(key)
          user.prefListExtend.remove(key)
        }
        value += row._2.length.toDouble
        user.prefListExtend.put(key,value)
      }
      if(user.prefListExtend.size()>20){

      }
    })

    users(user.UserID,user.UserName,"","",0,0,user.prefListExtend.toString,"","","","","","","")
  }

  def jsonPrefListtoMap (srcJson: String): CustomizedHashMap[String, CustomizedHashMap[String, Double]] = {

    if(objectMapper == null) {
      objectMapper = new ObjectMapper
    }
    var result = new CustomizedHashMap[String, CustomizedHashMap[String, Double]]()
    try{
      //println(srcJson)
      var map:CustomizedHashMap[String, CustomizedHashMap[String, Double]] = objectMapper.readValue(srcJson, new TypeReference[CustomizedHashMap[String, CustomizedHashMap[String, Double]]]() {})
      var iterator = map.keySet.iterator()
      while(iterator.hasNext){
        var moduleId = iterator.next()
        if(map.get(moduleId).toString != "{}")
          result.put(moduleId,map.get(moduleId))
      }
    }
    catch {
      case e: JsonParseException =>
        e.printStackTrace()
      case e: JsonMappingException =>
        // TODO Auto-generated catch block
        e.printStackTrace()
      case e: IOException =>
        e.printStackTrace()
    }
    return result
  }

  def jsonArticlePrefListtoMap(srcJson: String):CustomizedHashMap[String, Double] = {
    if(objectMapper == null) {
      objectMapper = new ObjectMapper
    }
    var result = new CustomizedHashMap[String, Double]()

    try{
      //
      //println(srcJson)
      var map:CustomizedHashMap[String, Double] = objectMapper.readValue(srcJson, new TypeReference[CustomizedHashMap[String, Double]]() {})
      result = map
    }
    catch {
      case e: JsonParseException =>
        e.printStackTrace()
      case e: JsonMappingException =>
        // TODO Auto-generated catch block
        e.printStackTrace()
      case e: IOException =>
        e.printStackTrace()
    }
    return result
  }

  def jsonLoglisttoLogMap(srcJSON:String):CustomizedHashMap[String, String]={
    if(objectMapper == null) objectMapper = new ObjectMapper
    var map: CustomizedHashMap[String, String] = objectMapper.readValue(srcJSON,new TypeReference[CustomizedHashMap[String, String]] {})
    map
  }

  def intervalTime(startDate:Date,endDate:Date): Long ={
    var between = endDate.getTime - startDate.getTime
    val day: Long = between / 1000 / 3600 / 24
    day
  }

  def getMatchValue(map:CustomizedHashMap[String, Double],list:CustomizedHashMap[String, Double]): Double ={

    var matchValue = 0D
    import scala.collection.JavaConversions._
    for (keyword <- list) {
      if (map.contains(keyword._1)) matchValue += keyword._2 * map.get(keyword._1)
    }

    matchValue
  }

  def removeZeroItem(arr: ArrayBuffer[(Long, Double)]): Unit ={
    arr.filter(tuple=>tuple._2>0)
  }

  def compare(t1:(BigInt,String,Long,String,Double),t2:(BigInt,String,Long,String,Double)): Boolean ={
    t1._5.compareTo(t2._5) > 0
  }

  def formatUsers(line:String,logTime:Broadcast[String]):users={
    val tokens = line.split(SEP)
    if(tokens.size == 14){
      //UserTemp(tokens(0).toLong,tokens(1),"{}",jsonPrefListtoMap("{}"),tokens(3))
      //println(tokens.length)
      var UserID = tokens(0).toInt
      var UserName = tokens(1)
      var LawOfworkAndRest = if(tokens(2) == "\"\"") "" else  tokens(2)
      var Area = if(tokens(3) == "\"\"") "" else  tokens(3)
      var Age = 0
      var Gender = 0
      var SingleArticleInterest = if(tokens(6) == "\"\"" || tokens(6).isEmpty || tokens(6) == "{}") "{}" else tokens(6).substring(1,tokens(6).length-1).replace("\\","")
      var BooksInterests = if(tokens(7) == "\"\"" || tokens(7).isEmpty || tokens(7) == "{}") "{}" else tokens(7).substring(1,tokens(7).length-1).replace("\\","")
      var JournalsInterests = if(tokens(8) == "\"\"" || tokens(8).isEmpty || tokens(8) == "{}") "{}" else tokens(8).substring(1,tokens(8).length-1).replace("\\","")
      var ReferenceBookInterests = if(tokens(9) == "\"\"" || tokens(9).isEmpty || tokens(9) == "{}") "{}" else tokens(9).substring(1,tokens(9).length-1).replace("\\","")
      var CustomerPurchasingPowerInterests = if(tokens(10) == "\"\"" || tokens(10).isEmpty || tokens(10) == "{}") "{}" else tokens(10).substring(1,tokens(10).length-1).replace("\\","")
      var ProductviscosityInterests = if(tokens(11) == "\"\"" || tokens(11).isEmpty || tokens(11) == "{}") "{}" else tokens(11).substring(1,tokens(11).length-1).replace("\\","")
      var PurchaseIntentionInterests = if(tokens(12) == "\"\"" || tokens(12).isEmpty || tokens(12) == "{}") "{}" else tokens(12).substring(1,tokens(12).length-1).replace("\\","")
      users(UserID,UserName,LawOfworkAndRest,Area,Age,Gender,SingleArticleInterest,BooksInterests,JournalsInterests,ReferenceBookInterests,CustomerPurchasingPowerInterests,ProductviscosityInterests,PurchaseIntentionInterests,logTime.value)
    }else{
      users(0,"","","",0,0,"","","","","","","",logTime.value)
    }
  }

  def formatNews(line:String): NewsTemp ={

    val tokens = line.split(SEP)

    var title = tokens(2)
    var content = ""

    var jsonStr = tokens(8).substring(1,tokens(8).length-1).replace("\\","")
    var map:CustomizedHashMap[String,Double] = jsonPrefListtoMap(jsonStr).get(tokens(6))

    /**/
    if(map == null){

      val keywords = TFIDFNEW.getTFIDE(title, 10).iterator()

      map = new CustomizedHashMap[String,Double]()

      while (keywords.hasNext) {
        var keyword = keywords.next()
        val name = keyword.getName
        val score = keyword.getTfidfvalue
        map.put(name,score)
      }
    }
    NewsTemp(tokens(0).toLong, "", tokens(6), title, tokens(5),map)
  }

  def formatJournalBaseInfo(line:String): JournalBaseTemp ={

    val tokens = line.split(SEP)

    var title = tokens(2)
    var content = ""

    var jsonStr = tokens(8).substring(1,tokens(8).length-1).replace("\\","")
    var map:CustomizedHashMap[String,Double] = jsonPrefListtoMap(jsonStr).get(tokens(6))

    /**/
    if(map == null){

      val keywords = TFIDFNEW.getTFIDE(title, 10).iterator()

      map = new CustomizedHashMap[String,Double]()

      while (keywords.hasNext) {
        var keyword = keywords.next()
        val name = keyword.getName
        val score = keyword.getTfidfvalue
        map.put(name,score)
      }
    }
    JournalBaseTemp(tokens(0).toLong,tokens(1), "", tokens(7), title, tokens(6),jsonStr)
  }

  def formatRecommandTuple(user:UserTemp,newsBroadCast:Broadcast[Array[NewsTemp]]): ArrayBuffer[(BigInt,String, Long,String, Double)] ={
    var tempMatchArr = new ArrayBuffer[(BigInt,String, Long,String, Double)]()
    var ite = newsBroadCast.value.iterator
    while (ite.hasNext) {
      val news = ite.next
      val newsId = news.id
      val moduleId = news.module_id

      var map = user.prefListExtend.get(moduleId)

      if (null != map) {
        val tuple: (BigInt,String, Long,String, Double) = (user.UserID,user.UserName, newsId,news.title, getMatchValue(map, news.keywords))
        tempMatchArr += tuple
      }
    }
    // 去除匹配值为0的项目,并排序
    var sortedTuples: ArrayBuffer[(BigInt,String, Long,String, Double)] = tempMatchArr.filter(tuple => tuple._5 > 0).sortWith(compare)

    if (sortedTuples.length > 0) {
      //暂时不操作
      //过滤掉已经推荐过的新闻
      //RecommendKit.filterReccedNews(toBeRecommended, user.id)
      //过滤掉用户已经看过的新闻
      //RecommendKit.filterBrowsedNews(toBeRecommended, user.id)
      //如果可推荐新闻数目超过了系统默认为CB算法设置的单日推荐上限数（N），则去掉一部分多余的可推荐新闻，剩下的N个新闻才进行推荐
    }
    if (sortedTuples.length > 10)
      sortedTuples = sortedTuples.take(10)
    sortedTuples
  }

  def formatUserViewLogs(line:String): Log_Temp ={
    val tokens = line.split("::")

    //val date1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(tokens(1))

    val map = jsonPrefListtoMap(tokens(4))

    Log_Temp(tokens(0),tokens(1),tokens(2),"",tokens(3),map.get(tokens(3)))
  }

  def formatUserLog(line:String):UserLogObj={
    val strings = line.split("\\d] ")
    var obj: UserLogObj = null
    if(strings.size == 2){
      //导入隐式值
      implicit val formats = DefaultFormats
      obj  = parse(strings(1),false).extract[UserLogObj]
    }else{
      println(line)
    }
    obj
  }

  def formateBookInfo(line:String):BookBaseInfo={
    val strings = line.split(SEP)
    if(strings.size == 5){
      BookBaseInfo(strings(0),strings(1),strings(2),strings(3),strings(4).replace("\\",""))
    }else{
      BookBaseInfo("","","","","")
    }
  }

  def getNowStr():String={
    var now =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
    now
  }

  def getToday():String={
    var today = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    today
  }

  def getYesterday():String={
    var today = new Date()
    var yesterday = new SimpleDateFormat("yyyy-MM-dd").format( today.getTime -  86400000L)
    yesterday
  }

  def recommand(userList:RDD[UserTemp],newsBroadCast:Broadcast[Array[NewsTemp]]): RDD[recommendations] ={

    val recommandRDD: RDD[recommendations] = userList.map(user => formatRecommandTuple(user, newsBroadCast)).flatMap(_.toList).map(tuple => {
      recommendations(tuple._1, tuple._2, tuple._3, tuple._4, tuple._5)
    })
    recommandRDD
  }

  def filterLog(logObj: UserLogObj):Boolean ={
      var res = true
      import scala.util.control.Breaks._
      breakable{
        for(ua<- spiderArray){
          res = logObj != null && !logObj.ua.toLowerCase.contains(ua.toLowerCase)
          if(!res) break
        }
      }
    res
  }

  def getNewUserList(userDataFrame:DataFrame,newsLogDataFrame:DataFrame,logTime:Broadcast[String]):RDD[users]={

    val userRDD = userDataFrame.select("UserName").rdd.map(row=>{
      row.getAs[String](0)
    }).persist()

    val newUserList = newsLogDataFrame.select("un").distinct().rdd.map(row => {
      row.getAs[String](0)
    }).subtract(userRDD).map(str => {
      users(0, str, "", "", 0, 0, "{}", "{}", "{}", "{}", "{}", "{}", "{}", logTime.value)
    })
    //userRDD.unpersist()

    newUserList

  }

  def getNewGuestUserList(userDataFrame:DataFrame,newsLogDataFrame:DataFrame,logTime:Broadcast[String]):RDD[users]={

    val userRDD = userDataFrame.select("UserName").rdd.map(row=>{
      row.getAs[String](0)
    }).persist()

    val newUserList = newsLogDataFrame.select("gki").distinct().rdd.map(row => {
      row.getAs[String](0)
    }).subtract(userRDD).map(str => {
      users(0, str, "", "", 0, 0, "{}", "{}", "{}", "{}", "{}", "{}", "{}", logTime.value)
    })
    //userRDD.unpersist()

    newUserList

  }
}
