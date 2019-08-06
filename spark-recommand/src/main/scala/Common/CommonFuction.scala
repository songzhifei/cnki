//package Common

import java.io.{FileInputStream, IOException}
import java.text.SimpleDateFormat
import java.util
import java.util._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.codehaus.jackson.JsonParseException
import org.codehaus.jackson.`type`.TypeReference
import org.codehaus.jackson.map.{JsonMappingException, ObjectMapper}
import scala.collection.mutable.ArrayBuffer
import CommonObj.{UserArticleTempNew, _}
import com.alibaba.fastjson.JSON

/**
  *
  * 公用方法类
  *
  */
object CommonFuction {


  /**
    * json string反序列化为对象
    */
  var objectMapper:ObjectMapper = null

  /*
  * 分隔符
  */
  var SEP = "&"

  /**
    * 权重最小值
    */
  var MinWeight = 0.01
  /**
    * 关键词最小数量
    */
  var MinKeywordsCount = 10

  /*
  *
  * 默认衰减系数
   */
  var BaseAttenuationCoefficient = 0.9

  /**
    * 日志需过滤的的爬虫标识
    *
    */
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

  /**
    *
    * mysql URL
    */
  var MysqlURL = ""

  /**
    * mysql 用户名
    */
  var UserName = "root"

  /**
    * mysql 密码
    */
  var Password = "root"

  /**
    * mysql dataTable
    */
  var DataTable = "huge.cnki_user_portrait"

  /**
    * 获取配置文件的相应属性
    */
  def getProperty(): Unit ={
    var props = new Properties()
    props.load(new FileInputStream("property.yml"))
    if(!props.getProperty("MinWeight").isEmpty)
      MinWeight = props.getProperty("MinWeight").toDouble
    if(!props.getProperty("MinKeywordsCount").isEmpty)
      MinKeywordsCount = props.getProperty("MinKeywordsCount").toInt
    if(!props.getProperty("BaseAttenuationCoefficient").isEmpty)
      BaseAttenuationCoefficient = props.getProperty("BaseAttenuationCoefficient").toDouble
    if(!props.getProperty("MysqlURL").isEmpty)
      MysqlURL = props.getProperty("MysqlURL")
    if(!props.getProperty("UserName").isEmpty)
      UserName = props.getProperty("UserName")
    if(!props.getProperty("Password").isEmpty)
      Password = props.getProperty("Password")
    if(!props.getProperty("DataTable").isEmpty)
      DataTable = props.getProperty("DataTable")
  }

  /**
    * 根据actiontype获取相应的权重
    * @param str
    * @return
    */
  def getWeight(str:String):Double={
    var result = 1D
    str match {
      case "browse" => result = 1D
      case "buy" => result = 2D
      case "read" => result = 1D
      case "collect" => result = 1D
      case "concern" => result = 1D
      case "comment" => result = 1D
      case "search" => result = 1D
      case _ => result = 1L
    }
    result
  }

  /**
    * 按照默认衰减系数衰减算法(guestmain中引用)
    * @param user
    * @return
    */
  def autoDecRefresh(user:UserTemp): UserTemp ={

    //用于删除喜好值过低的关键词
    val keywordToDelete = new ArrayList[String]

    val map =user.prefListExtend

    var times = 1L

    if(!user.latest_log_time.isEmpty && user.latest_log_time != "\"\"")
      times = intervalTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(user.latest_log_time),new Date())

    for(i<- 0 to times.toInt - 1) BaseAttenuationCoefficient *= BaseAttenuationCoefficient

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
          val result = (moduleMap.get(key) * BaseAttenuationCoefficient).formatted("%.4f").toDouble
          if (result < 0.01) keywordToDelete.add(key)
          moduleMap.put(key, result.toDouble)
        }
      }
      import scala.collection.JavaConversions._
      for (deleteKey <- keywordToDelete) {
        moduleMap.remove(deleteKey)
      }
      //newMap.put()
      keywordToDelete.clear()

      if(!moduleMap.isEmpty) newMap.put(moduleId,moduleMap)
    }

    UserTemp(user.UserID,user.UserName,user.prefList,newMap,user.latest_log_time)
  }

  /**
    * 用户最近关注的文章按照牛顿衰减算法衰减方法
    * @param oldMap
    * @return
    */
  def autoDecRefreshArticleRecentInterests(oldMap:CustomizedHashMap[String, CustomizedHashMap[String,CustomizedKeyWord]]): CustomizedHashMap[String, CustomizedHashMap[String,CustomizedKeyWord]] ={
    import scala.collection.JavaConversions._
    var moduleId = ""
    var oldMapIterator = oldMap.keySet().iterator()
    var stringToWord:CustomizedHashMap[String,CustomizedKeyWord] = new CustomizedHashMap[String,CustomizedKeyWord]
    var customizedKeyWord = new CustomizedKeyWord()
    var keywordsToDelete = new ArrayList[String]
    var times = 1L
    var keyword = ""
    var tempWeight = 1D
    //获取关键词总个数
    var keywordsTotal = 0
    if(oldMap.size()>0){
      while(oldMapIterator.hasNext){
         moduleId = oldMapIterator.next()
         keywordsTotal += oldMap.get(moduleId).size()
      }
      //当关键词个数小于设定阈值时，权重不再进行衰减,暂时设置0
      if(keywordsTotal > 0){//衰减规则
        oldMapIterator = oldMap.keySet().iterator()
        while(oldMapIterator.hasNext){

          moduleId = oldMapIterator.next()

          stringToWord = oldMap.get(moduleId)
          //oldMap.remove(moduleId)
          val stringToWordIterator = stringToWord.keySet().iterator()

          while(stringToWordIterator.hasNext){
            keyword = stringToWordIterator.next()
            customizedKeyWord = stringToWord.get(keyword)

            //stringToWord.remove(keyword)

            times = intervalTime(new Date(customizedKeyWord.getDateTime.toLong),new Date())

            tempWeight = customizedKeyWord.getWeight * TagAttenuationUtils.get30TDaysConstant(times.toInt,MinWeight)

            /*
            import scala.util.control.Breaks._
            breakable{
              for(i<- 0 to times.toInt - 1){

                tempWeight *= BaseAttenuationCoefficient

                if(tempWeight <=0.001) break()
              }
            }
            * */

            var newWeight = (if(tempWeight <=0.001) 0.001 else tempWeight).formatted("%.4f").toDouble //设置关键词比重最小阈值，否则可能无限的接近0

            customizedKeyWord.setWeight(newWeight)
            //customizedKeyWord.setDateTime((new Date()).getTime.toString)
            //新的关键词比重大于最小阈值或者关键词总数小于最小关键词总数时，不再删除权重值低的关键词
            //if(newWeight >= minWeight || keywordsTotal-1 <= minKeywordsCount) stringToWord.put(keyword,customizedKeyWord)
            if(newWeight < MinWeight && keywordsTotal-1 > MinKeywordsCount){
              keywordsTotal = keywordsTotal - 1
              keywordsToDelete.add(keyword)
            }
          }
          for(keyword <- keywordsToDelete){
            stringToWord.remove(keyword)
          }
          //关键词按照不同比重排序
          //stringToWord = orderByHashMap(stringToWord,true)
          keywordsToDelete.clear()
        }

      }

      //把学科分类中没有关键词的分类删掉
      /** */
      oldMapIterator = oldMap.keySet().iterator()
      while (oldMapIterator.hasNext){
        moduleId = oldMapIterator.next()
        if(oldMap.get(moduleId).size()<=0) keywordsToDelete.add(moduleId)
      }
      for(moduleId<-keywordsToDelete){
        oldMap.remove(moduleId)
      }
    }
    oldMap
  }

  /**
    * 用户关注的作者按照牛顿衰减算法衰减方法
    * @param oldMap
    * @return
    */
  def autoDecRefreshAuthorRecentInterests(oldMap:CustomizedHashMap[String, CustomizedKeyWord]): CustomizedHashMap[String, CustomizedKeyWord] ={
    import scala.collection.JavaConversions._
    var authorId = ""
    var oldMapIterator = oldMap.keySet().iterator()
    var customizedKeyWord = new CustomizedKeyWord()
    var keywordsToDelete = new ArrayList[String]
    var times = 1L
    var tempWeight = 1D
    //获取关键词总个数
    var keywordsTotal = 0
    //当关键词个数小于设定阈值时，权重不再进行衰减,暂时设置0
    if(oldMap.size() > 0){//衰减规则

      oldMapIterator = oldMap.keySet().iterator()
      while(oldMapIterator.hasNext){

        authorId = oldMapIterator.next()

        customizedKeyWord = oldMap.get(authorId)

        times = intervalTime(new Date(customizedKeyWord.getDateTime.toLong),new Date())

        tempWeight = customizedKeyWord.getWeight * TagAttenuationUtils.get30TDaysConstant(times.toInt,MinWeight)
        /*
        import scala.util.control.Breaks._
        breakable{
          for(i<- 0 to times.toInt - 1){

            tempWeight *= BaseAttenuationCoefficient

            if(tempWeight <=0.001) break()
          }
        }
        * */

        var newWeight = (if(tempWeight <=0.001) 0.001 else tempWeight).formatted("%.4f").toDouble //设置关键词比重最小阈值，否则可能无限的接近0

        customizedKeyWord.setWeight(newWeight)
        customizedKeyWord.setDateTime((new Date()).getTime.toString)
        //新的关键词比重大于最小阈值或者关键词总数小于最小关键词总数时，不再删除权重值低的关键词
        //if(newWeight >= minWeight || keywordsTotal-1 <= minKeywordsCount) stringToWord.put(keyword,customizedKeyWord)
        if(newWeight < MinWeight && keywordsTotal-1 > MinKeywordsCount){
          keywordsTotal = keywordsTotal - 1
          keywordsToDelete.add(authorId)
        }

        for(keyword <- keywordsToDelete){
          oldMap.remove(keyword)
        }
      }
    }
    oldMap
  }

  /**
    * 文章按照默认衰减系数衰减(guestmain中引用)
    * @param user
    * @return
    */
  def autoDecRefreshArticleInterests(user:UserArticleTemp): UserArticleTemp ={

    //用于删除喜好值过低的关键词
    val keywordToDelete = new ArrayList[String]

    val map =user.prefListExtend

    var baseAttenuationCoefficient = 0.9

    var times = 1L

    if(!user.latest_log_time.isEmpty && user.latest_log_time != "\"\"" && user.latest_log_time !="{}")
      times = intervalTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(user.latest_log_time),new Date())

    for(i<- 0 to times.toInt - 1) BaseAttenuationCoefficient *= BaseAttenuationCoefficient

    var newMap:CustomizedHashMap[String, Double] = new CustomizedHashMap[String, Double]


    val ite = map.keySet.iterator

    while (ite.hasNext){
      //用户对应模块的喜好不为空
      val key = ite.next

      var value = map.get(key)

      value = value * BaseAttenuationCoefficient

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


  /**
    * 根据日志 + 历史用户画像 = 生成最新用户画像
    * @param user
    * @param logsBroadCast
    * @param logTime
    * @return
    */
  def getUserPortrait(user:UserTemp,logsBroadCast:Broadcast[collection.Map[String, Array[Log_Temp]]],logTime:Broadcast[String]): UserTemp ={
    val logsList: Array[Log_Temp] = logsBroadCast.value.get(user.UserName).getOrElse(new Array[Log_Temp](0))
    //println("处理前的rateMap：" + user.prefListExtend.toString)
    if (logsList.length >0) {
      //1.3. 根据用户画像和浏览历史更新各个用户的用户兴趣标签
      for(logs <- logsList){
        val moduleIds = logs.module_id.split(";")
        for(moduleIdTemp<-moduleIds){

          //var moduleId = moduleIdTemp.substring(0,4)
          var moduleId = moduleIdTemp
          var rateMap: CustomizedHashMap[String, Double] = user.prefListExtend.get(moduleId)

          //获取log的keywords列表
          var logMap = new CustomizedHashMap[String,Double]()
          if(!logs.keywords.isEmpty){
            val keywordsTemp = logs.keywords.replaceAll("[< >]","").split("[\\, ; \\$]") //关键词中可能含有多个不同的分隔符
            for(keyword <- keywordsTemp){
              if(!logMap.containsKey(keyword) && !keyword.isEmpty) logMap.put(keyword,1)
            }
          }
          //println("logMap：" + logMap)
          if(logMap !=null && logMap.size > 0){
            if(rateMap ==null) {
              rateMap = new CustomizedHashMap[String,Double]()
            }else{
              user.prefListExtend.remove(moduleId)
            }

            user.prefListExtend.put(moduleId,rateMap)

            val keywordIte = logMap.keySet().iterator()
            while(keywordIte.hasNext){
              val name = keywordIte.next()
              val score = logMap.get(name)
              if(rateMap.containsKey(name))
                rateMap.put(name, rateMap.get(name) + score)
              else
                rateMap.put(name, score)
            }
          }
        }
        //println("logMap：" + user.prefListExtend.get(logs.module_id))
      }
    }
    //println("处理后的logMap：" + user.prefListExtend.toString)

    UserTemp(user.UserID,user.UserName,user.prefList,user.prefListExtend,logTime.value)
    //users(user.id.toInt,user.username,"","",0,0,"","",user.prefListExtend.toString,"","","","",logTime.value)
  }

  /**
    * 根据用户文章行为日志 + 历史画像 = 最新画像
    * @param user
    * @param newsBroadCast
    * @return
    */
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

  /**
    * jsonstring格式反序列化为对象CustomizedHashMap[String, CustomizedHashMap[String, Double] （已弃用）
    * @param srcJson
    * @return
    */
  def jsonPrefListtoMap (srcJson: String): CustomizedHashMap[String, CustomizedHashMap[String, Double]] = {

    if(objectMapper == null) {
      objectMapper = new ObjectMapper
    }
    var result = new CustomizedHashMap[String, CustomizedHashMap[String, Double]]()
    try{
      //println(srcJson)
      if(srcJson != "{}"){
        var map:CustomizedHashMap[String, CustomizedHashMap[String, Double]] = objectMapper.readValue(srcJson, new TypeReference[CustomizedHashMap[String, CustomizedHashMap[String, Double]]]() {})
        var iterator = map.keySet.iterator()
        while(iterator.hasNext){
          var moduleId = iterator.next()
          if(map.get(moduleId).toString != "{}")
            result.put(moduleId,map.get(moduleId))
        }
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

  /**
    * jsonstring格式反序列化为对象CustomizedHashMap[String, CustomizedHashMap[String, CustomizedKeyWord] 最新
    * @param srcJson
    * @return
    */
  def jsonPrefListtoMapNew (srcJson: String): CustomizedHashMap[String, CustomizedHashMap[String, CustomizedKeyWord]] = {

    if(objectMapper == null) {
      objectMapper = new ObjectMapper
    }
    var result = new CustomizedHashMap[String, CustomizedHashMap[String, CustomizedKeyWord]]()
    try{
      //println(srcJson)
      if(srcJson != "{}" && srcJson != ""){
        var map:CustomizedHashMap[String, CustomizedHashMap[String, CustomizedKeyWord]] = objectMapper.readValue(srcJson, new TypeReference[CustomizedHashMap[String, CustomizedHashMap[String, CustomizedKeyWord]]]() {})
        var iterator = map.keySet.iterator()
        while(iterator.hasNext){
          var moduleId = iterator.next()
          if(map.get(moduleId).toString != "{}")
            result.put(moduleId,map.get(moduleId))
        }
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

  /**
    * jsonstring反序列化为CustomizedHashMap[String, CustomizedKeyWord] 最新
    * @param srcJson
    * @return
    */
  def jsonConcernedSubjectListToMap(srcJson: String):CustomizedHashMap[String, CustomizedKeyWord] = {
    if(objectMapper == null) {
      objectMapper = new ObjectMapper
    }
    var result = new CustomizedHashMap[String, CustomizedKeyWord]()

    try{
      //println(srcJson)
      if(srcJson != "{}" && srcJson != ""){
        var map:CustomizedHashMap[String, CustomizedKeyWord] = objectMapper.readValue(srcJson, new TypeReference[CustomizedHashMap[String, CustomizedKeyWord]]() {})
        result = map
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

  /**
    * jsonstring反序列化为CustomizedHashMap[String, Double]（guest main中引用）
    * @param srcJson
    * @return
    */
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

  /**
    * 计算两个时间之间的相间隔天数
    * @param startDate
    * @param endDate
    * @return
    */
  def intervalTime(startDate:Date,endDate:Date): Long ={
    var between = endDate.getTime - startDate.getTime
    val day: Long = between / 1000 / 3600 / 24
    day
  }

  /**
    *基于内容的推荐（从用户的喜爱列表中寻找待推荐列表中与之匹配的推荐数据，暂时不用）
    * @param map
    * @param list
    * @return
    */
  def getMatchValue(map:CustomizedHashMap[String, Double],list:CustomizedHashMap[String, Double]): Double ={

    var matchValue = 0D
    import scala.collection.JavaConversions._
    for (keyword <- list) {
      if (map.contains(keyword._1)) matchValue += keyword._2 * map.get(keyword._1)
    }

    matchValue
  }

  /**
    * 过滤掉关键词比重小于等于0的数据
    * @param arr
    */
  def removeZeroItem(arr: ArrayBuffer[(Long, Double)]): Unit ={
    arr.filter(tuple=>tuple._2>0)
  }

  /**
    * 自定义二维tuple比较大小方法
    * @param t1
    * @param t2
    * @return
    */
  def compare(t1:(BigInt,String,Long,String,Double),t2:(BigInt,String,Long,String,Double)): Boolean ={
    t1._5.compareTo(t2._5) > 0
  }

  /**
    * 格式化用户画像数据（已弃用）
    * @param line
    * @param logTime
    * @return
    */
  def formatUsers(line:String,logTime:Broadcast[String]):users={
    val tokens = line.split(SEP)
    //println(tokens.size)
    if(tokens.size == 14){
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
      //users(UserID,UserName,LawOfworkAndRest,LawOfworkAndRest,Age,Gender,"{}","{}","{}","{}","{}","{}","{}",logTime.value)
    }else{
      users(0,"","","",0,0,"","","","","","","",logTime.value)
    }
  }

  /**
    * 格式化用户画像方法（最新）
    * @param line
    * @param logTime
    * @return
    */
  def formatUsersNew(line:String,logTime:Broadcast[String]):usersNew={
    val tokens = line.split(SEP)
    //println(tokens.size)
    if(tokens.size >= 14){
      //println(tokens.length)
      var UserID = 0
      var UserName = tokens(0)
      var LawOfworkAndRest = if(tokens(1) == "\"\"") "" else  tokens(1)
      var Area = if(tokens(2) == "\"\"") "" else  tokens(2)
      var Age = 0
      var Gender = 0
      var ConcernedSubject = if(tokens(5) == "\"\"" || tokens(5).isEmpty || tokens(5) == "{}") "{}" else tokens(5).substring(1,tokens(5).length-1).replace("\\","")
      var SubConcernedSubject = if(tokens(6) == "\"\"" || tokens(6).isEmpty || tokens(6) == "{}") "{}" else tokens(6).substring(1,tokens(6).length-1).replace("\\","")
      var SingleArticleTotalInterest = if(tokens(7) == "\"\"" || tokens(7).isEmpty || tokens(7) == "{}") "{}" else tokens(7).substring(1,tokens(7).length-1).replace("\\","")
      var BooksInterests = ""
      var SingleArticleRecentInterest  = if(tokens(8) == "\"\"" || tokens(8).isEmpty || tokens(8) == "{}") "{}" else tokens(8).substring(1,tokens(8).length-1).replace("\\","")
      var TotalRelatedAuthor  = if(tokens(9) == "\"\"" || tokens(9).isEmpty || tokens(9) == "{}") "{}" else tokens(9).substring(1,tokens(9).length-1).replace("\\","")
      var RecentRelatedAuthor  = if(tokens(10) == "\"\"" || tokens(10).isEmpty || tokens(10) == "{}") "{}" else tokens(10).substring(1,tokens(10).length-1).replace("\\","")
      var JournalsInterests = ""
      var ReferenceBookInterests = ""
      var CustomerPurchasingPowerInterests = ""
      var ProductviscosityInterests = ""
      var PurchaseIntentionInterests = ""
      usersNew(UserName,LawOfworkAndRest,Area,Age,Gender,ConcernedSubject,SubConcernedSubject,SingleArticleTotalInterest,SingleArticleRecentInterest,TotalRelatedAuthor,RecentRelatedAuthor,BooksInterests,JournalsInterests,ReferenceBookInterests,CustomerPurchasingPowerInterests,logTime.value)
      //users(UserID,UserName,LawOfworkAndRest,LawOfworkAndRest,Age,Gender,"{}","{}","{}","{}","{}","{}","{}",logTime.value)
    }else{
      usersNew("","","",0,0,"","","","","","","","","","",logTime.value)
    }
  }

  /**
    * 格式化用户相关的标签（已弃用）
    * @param line
    * @param logTime
    * @return
    */
  def formatRelatedLabel(line:String,logTime:Broadcast[String]):RelatedLabel={
    val tokens = line.split(SEP)
    if(tokens.size >= 3){
      var UserName = tokens(0)

      var TotalRelatedAuthor  = if(tokens(1) == "\"\"" || tokens(1).isEmpty || tokens(1) == "{}") "{}" else tokens(1).substring(1,tokens(1).length-1).replace("\\","")
      var RecentRelatedAuthor  = if(tokens(2) == "\"\"" || tokens(2).isEmpty || tokens(2) == "{}") "{}" else tokens(2).substring(2,tokens(2).length-1).replace("\\","")
      RelatedLabel(UserName,TotalRelatedAuthor,RecentRelatedAuthor)
    }else{
      //usersNew(0,"","","",0,0,"","","","","","","","","","","",logTime.value)
      RelatedLabel("","","")
    }
  }

  /**
    * 基于内容进行的推荐，暂时无用
    * @param line
    * @return
    */
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

  /**
    * 格式化用户日志jsonstring 为 UserLogObj
    * @param line
    * @return
    */
  def formatUserLog(line:String):UserLogObj={
    val strings = line.split("\\d] ")
    var obj: UserLogObj = null
    if(strings.size == 2){
      /*
      * 暂时注释掉，日志中的json key值不确定，无法使用该方法，改用import com.alibaba.fastjson.JSON
      //导入隐式值
      implicit val formats = DefaultFormats
      obj  = parse(strings(1),false).extract[UserLogObj]
      * */
      val jSONObject = JSON.parseObject(strings(1))
      obj = UserLogObj(
        jSONObject.getOrDefault("vt","").toString
        ,jSONObject.getOrDefault("un","").toString
        ,jSONObject.getOrDefault("gki","").toString
        ,jSONObject.getOrDefault("ac","").toString
        ,jSONObject.getOrDefault("ro","").toString
        ,jSONObject.getOrDefault("klc","").toString
        ,jSONObject.getOrDefault("ri","").toString
        ,jSONObject.getOrDefault("rkd","").toString
        ,jSONObject.getOrDefault("sw","").toString
        ,jSONObject.getOrDefault("p","").toString
        ,jSONObject.getOrDefault("ci","").toString
        ,jSONObject.getOrDefault("ua","").toString
        ,jSONObject.getOrDefault("rcc","").toString
        ,jSONObject.getOrDefault("rccc","").toString
        ,jSONObject.getOrDefault("pfc","").toString
        ,jSONObject.getOrDefault("dt","").toString
        ,jSONObject.getOrDefault("di","").toString
        ,jSONObject.getOrDefault("au","").toString
        ,jSONObject.getOrDefault("jg","").toString
        ,jSONObject.getOrDefault("sou","").toString)
    }else{
      println(line)
    }
    obj
  }

  /**
    * 获取当前时间，格式：yyyy-MM-dd HH:mm:ss
    * @return
    */
  def getNowStr():String={
    var now =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
    now
  }

  /**
    * 获取今天日期，格式：yyyy-MM-dd
    * @return
    */
  def getToday():String={
    var today = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    today
  }

  /**
    * 获取昨天日期，格式：yyyy-MM-dd
    * @return
    */
  def getYesterday():String={
    var today = new Date()
    var yesterday = new SimpleDateFormat("yyyy-MM-dd").format( today.getTime -  86400000L)
    yesterday
  }

  /**
    * 用户日志过滤不符合规则的内容
    * @param logObj
    * @return
    */
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

  /**
    * 根据日志获取最新的用户信息（由于用户信息表数据不全，日志里可能存在新增用户，所以需要从日志中提取一遍新用户）已弃用
    * @param userDataFrame
    * @param newsLogDataFrame
    * @param logTime
    * @return
    */
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

  /**
    * 根据日志获取最新的用户信息（由于用户信息表数据不全，日志里可能存在新增用户，所以需要从日志中提取一遍新用户）最新
    * @param userDataFrame
    * @param newsLogDataFrame
    * @param logTime
    * @return
    */
  def getNewUserListNew(userDataFrame:DataFrame,newsLogDataFrame:DataFrame,logTime:Broadcast[String]):RDD[usersNew]={

    val userRDD = userDataFrame.select("UserName").rdd.map(row=>{
      row.getAs[String](0).replaceAll("[\\ \" ]","")
    }).filter(!_.isEmpty).persist()

    val newUserList = newsLogDataFrame.select("un").distinct().rdd.map(row => {
      row.getAs[String](0).replaceAll("[\\ \" ]","")
    }).filter(!_.isEmpty).subtract(userRDD).map(str => {
      usersNew(str, "", "", 0, 0, "{}","{}","{}","{}", "{}", "{}","{}", "{}", "{}", "{}", logTime.value)
    })
    //userRDD.unpersist()

    newUserList

  }

  /**
    * 根据日志获取最新的匿名用户（原因如上）
    * @param userDataFrame
    * @param newsLogDataFrame
    * @param logTime
    * @return
    */
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

  /**
    * 根据文章行为日志生成用户日志画像（已弃用）
    * @param articleLogDataFrame
    * @return
    */
  def getArticleLogInterests(articleLogDataFrame:DataFrame): RDD[UserArticleTempNew] ={
    //根据log获取用户标签
    var userArticleTempNew = articleLogDataFrame
      .rdd
      .map(row => {
        var userName = row.getAs[String]("un")
        var actionType = row.getAs[String]("ac")
        var module_arr = row.getAs[String]("rcc").split(";")
        var keywordTemp = row.getAs[String]("rkd").replaceAll("[< >]","").split("\\$\\$\\$")
        val keywordArr = if(keywordTemp.length == 2) keywordTemp(1).split("[\\, ; \\$]") else if(keywordTemp.length == 1) keywordTemp(0).split("[\\, ; \\$]") else new Array[String](0) //关键词中可能含有多个不同的分隔符
        val visitTime = row.getAs[String]("vt")
        var newMap:CustomizedHashMap[String,CustomizedHashMap[String, CustomizedKeyWord]] = new CustomizedHashMap[String,CustomizedHashMap[String, CustomizedKeyWord]]

        for(module_idTemp <- module_arr){
          var module_id = module_idTemp.substring(0,4)
          val stringToWord: CustomizedHashMap[String, CustomizedKeyWord] = new CustomizedHashMap[String, CustomizedKeyWord]

          for(keyword<-keywordArr){
            if(!keyword.isEmpty){
              var keyWord = new CustomizedKeyWord(getWeight(actionType),visitTime)//记录关键词标签访问时间戳
              stringToWord.put(keyword.toLowerCase,keyWord)
            }
          }
          //当学科分类下的关键词不为空时才加入到用户喜欢分类中
          if(stringToWord.size() > 0) newMap.put(module_id,stringToWord)
        }
        UserArticleTempNew(0,userName,newMap.toString,newMap,"")
      })
    userArticleTempNew
  }

  /**
    * 根据文章行为日志生成用户日志画像（最新）
    * @param articleLogDataFrame
    * @return
    */
  def getArticleLogInterestsNew(articleLogDataFrame:DataFrame): RDD[(String,UserArticleTempNew)] ={
    //根据log获取用户标签
    var userArticleTempNew = articleLogDataFrame
      .rdd
      .map(row => {
        var userName = row.getAs[String]("un")
        var module_arr = row.getAs[String]("rcc").split(";")
        var keywordTemp = row.getAs[String]("rkd").replaceAll("[< >]","").split("\\$\\$\\$")
        val keywordArr = if(keywordTemp.length == 2) keywordTemp(1).split("[\\, ; \\$]") else if(keywordTemp.length == 1) keywordTemp(0).split("[\\, ; \\$]") else new Array[String](0) //关键词中可能含有多个不同的分隔符
        val visitTime = row.getAs[String]("vt")
        var newMap:CustomizedHashMap[String,CustomizedHashMap[String, CustomizedKeyWord]] = new CustomizedHashMap[String,CustomizedHashMap[String, CustomizedKeyWord]]

        for(module_idTemp <- module_arr){
          var module_id = module_idTemp.substring(0,4)
          val stringToWord: CustomizedHashMap[String, CustomizedKeyWord] = new CustomizedHashMap[String, CustomizedKeyWord]

          for(keyword<-keywordArr){
            if(!keyword.isEmpty){
              var keyWord = new CustomizedKeyWord(1D,visitTime)//记录关键词标签访问时间戳
              stringToWord.put(keyword.toLowerCase,keyWord)
            }
          }
          //当学科分类下的关键词不为空时才加入到用户喜欢分类中
          if(stringToWord.size() > 0) newMap.put(module_id,stringToWord)
        }
        (userName,UserArticleTempNew(0,userName,newMap.toString,newMap,""))
      })
    userArticleTempNew
  }

  /**
    * 根据文章行为日志生成 用户关注的学科维度的画像（已弃用）
    * @param articleLogDataFrame
    * @param isSub
    * @return
    */
  def getUserConceredSubjects(articleLogDataFrame:DataFrame,isSub:Boolean):RDD[UserConcernedSubjectTemp] = {
    val articleLogList = articleLogDataFrame
      .rdd
      .map(row => {
        var userName = row.getAs[String]("un")
        var actionType = row.getAs[String]("ac")
        var module_Str = row.getAs[String]("rcc")
        val visitTime = row.getAs[String]("vt")
        var newMap: CustomizedHashMap[String, CustomizedKeyWord] = new CustomizedHashMap[String, CustomizedKeyWord]
        val module_arr = module_Str.split("[\\, ;]")
        for (moduleTemp <- module_arr) {
          var module_id = if(!isSub) moduleTemp.substring(0, 4) else moduleTemp

          var customizedKeyWord = new CustomizedKeyWord(getWeight(actionType),visitTime)

          newMap.put(module_id, customizedKeyWord)
        }
        //println("newMap-------------------------"+FastJsonUtils.getBeanToJson(newMap))
        UserConcernedSubjectTemp(0, userName, newMap.toString, newMap, "")
      })
    articleLogList
  }

  /**
    * 根据文章行为日志生成 用户关注的学科维度的画像（最新）
    * @param articleLogDataFrame
    * @param isSub
    * @return
    */
  def getUserConceredSubjectsNew(articleLogDataFrame:DataFrame,isSub:Boolean):RDD[(String,UserConcernedSubjectTemp)] = {
    val articleLogList = articleLogDataFrame
      .rdd
      .map(row => {
        var userName = row.getAs[String]("un")
        var module_Str = row.getAs[String]("rcc")
        val visitTime = row.getAs[String]("vt")
        var newMap: CustomizedHashMap[String, CustomizedKeyWord] = new CustomizedHashMap[String, CustomizedKeyWord]
        val module_arr = module_Str.split("[\\, ;]")
        for (moduleTemp <- module_arr) {
          var module_id = if(!isSub) moduleTemp.substring(0, 4) else moduleTemp

          var customizedKeyWord = new CustomizedKeyWord(1D,visitTime)

          newMap.put(module_id, customizedKeyWord)
        }

        (userName,UserConcernedSubjectTemp(0, userName, newMap.toString, newMap, ""))
      })
    articleLogList
  }
  /*
  * 根据日志生成相关作者的dataFrame
  */
  def getRelatedAuthorFromLog(articleLogDataFrame:DataFrame):RDD[UserConcernedSubjectTemp] = {
    val relatedAuthorFromLogList = articleLogDataFrame
      .rdd
      .map(row => {
        var userName = row.getAs[String]("un")
        var actionType = row.getAs[String]("ac")
        var author_Str = row.getAs[String]("au")
        val visitTime = row.getAs[String]("vt")
        var newMap: CustomizedHashMap[String, CustomizedKeyWord] = new CustomizedHashMap[String, CustomizedKeyWord]
        var filter_author_arr = Array("整理","本报记者","通讯员")
        val author_arr = author_Str.replaceAll("[\\r \\n]","").replaceAll("  ",";").split("[\\, ， ;]").filter(str=> !filter_author_arr.contains(str) && !str.isEmpty)
        for (author <- author_arr) {
          var customizedKeyWord = new CustomizedKeyWord(getWeight(actionType),visitTime)

          newMap.put(author, customizedKeyWord)
        }
        UserConcernedSubjectTemp(0, userName, newMap.toString, newMap, "")
      })
    relatedAuthorFromLogList
  }

  /*
  * 根据日志生成搜索关键词的dataFrame
  * */
  def getSearchKeyWordFromLog(articleLogDataFrame:DataFrame):RDD[UserConcernedSubjectTemp] = {
    val searchKeyWordFromLog = articleLogDataFrame
      .rdd
      .map(row => {
        var userName = row.getAs[String]("un")
        var actionType = row.getAs[String]("ac")
        var searchKeyword = row.getAs[String]("sw")
        val visitTime = row.getAs[String]("vt")
        var newMap: CustomizedHashMap[String, CustomizedKeyWord] = new CustomizedHashMap[String, CustomizedKeyWord]
        var customizedKeyWord = new CustomizedKeyWord(getWeight(actionType),visitTime)

        newMap.put(searchKeyword, customizedKeyWord)
        UserConcernedSubjectTemp(0, userName, newMap.toString, newMap, "")
      })
    searchKeyWordFromLog
  }

  /**
    * 根据文章行为日志生成用户相关作者维度画像（最新）
    * @param articleLogDataFrame
    * @return
    */
  def getRelatedAuthorFromLogNew(articleLogDataFrame:DataFrame):RDD[(String,UserConcernedSubjectTemp)] = {
    val relatedAuthorFromLogList = articleLogDataFrame
      .rdd
      .map(row => {
        var userName = row.getAs[String]("un")
        var author_Str = row.getAs[String]("au")
        val visitTime = row.getAs[String]("vt")
        var newMap: CustomizedHashMap[String, CustomizedKeyWord] = new CustomizedHashMap[String, CustomizedKeyWord]
        var filter_author_arr = Array("整理","本报记者","通讯员")
        val author_arr = author_Str.replaceAll("[\\r \\n]","").replaceAll("  ",";").split("[\\, ;]").filter(str=> !filter_author_arr.contains(str) && !str.isEmpty)
        for (author <- author_arr) {
          var customizedKeyWord = new CustomizedKeyWord(1D,visitTime)

          newMap.put(author, customizedKeyWord)
        }
        (userName,UserConcernedSubjectTemp(0, userName, newMap.toString, newMap, ""))
      })
    relatedAuthorFromLogList
  }

  /**
    * 用户文章维度 旧画像+ 根据日志生成的画像 = 最新用户画像 （已弃用）
    * @param originalSingleArticleInterestExtend
    * @param userArticleTempNew
    * @param isTotalOrRecent
    * @param autoDecRefresh
    * @return
    */
  def unionOriginAndNewArticleLogInterests(originalSingleArticleInterestExtend:RDD[UserArticleTempNew],userArticleTempNew:RDD[UserArticleTempNew],isTotalOrRecent:Boolean,autoDecRefresh:Boolean):RDD[usersNew]={

    //原始用户标签和新生成标签合并
    originalSingleArticleInterestExtend.union(userArticleTempNew).groupBy(_.UserName).map(row=>{

      var userName = row._1

      var newMap:CustomizedHashMap[String, CustomizedHashMap[String,CustomizedKeyWord]] = new CustomizedHashMap[String, CustomizedHashMap[String,CustomizedKeyWord]]
      val iterator = row._2.iterator
      //每条数据以用户为单位
      while (iterator.hasNext){

        val userArticleTemp = iterator.next()
        if(newMap.size() <= 0 && userArticleTemp.prefListExtend.size() > 0){
          newMap = userArticleTemp.prefListExtend
        }else{
          val keyIterator = userArticleTemp.prefListExtend.keySet().iterator()
          //每条数据以学科分类为单位
          while (keyIterator.hasNext){
            var key = keyIterator.next()
            var value = userArticleTemp.prefListExtend.get(key)
            if(newMap.containsKey(key)){
              var oldMap: CustomizedHashMap[String, CustomizedKeyWord] = newMap.get(key)
              //先删除旧的map
              newMap.remove(key)
              //如果画像信息中包含该学科
              val iteratorKeywords = value.keySet().iterator()
              while (iteratorKeywords.hasNext){

                var keyword = iteratorKeywords.next()

                var keyWordObj = new CustomizedKeyWord(1D,value.get(keyword).getDateTime)
                if(oldMap.containsKey(keyword)){
                  keyWordObj.setWeight(oldMap.get(keyword).getWeight + 1D)
                  //只有当原始的关键词时间小于新的时间时才更新它
                  if(keyWordObj.getDateTime.toDouble < oldMap.get(keyword).getDateTime.toDouble)  keyWordObj.setDateTime(oldMap.get(keyword).getDateTime)
                  oldMap.remove(keyword)
                }
                oldMap.put(keyword,keyWordObj)
              }
              //增加更新好的map
              oldMap = orderByHashMap(oldMap,autoDecRefresh)
              newMap.put(key,oldMap)
            }else{//如果不包含直接追加即可
              value = orderByHashMap(value,autoDecRefresh)
              newMap.put(key,value)
            }
          }
        }
      }
      //FastJsonUtils.getBeanToJson(newMap)
      //如果设置自动衰减，则执行衰减方法
      if(!isTotalOrRecent){
        //newMap = autoDecRefreshArticleRecentInterests(newMap)

        usersNew(userName,"","",0,0,"","","",newMap.toString,"","","","","","","")
      }else{
        usersNew(userName,"","",0,0,"","",newMap.toString,"","","","","","","","")
      }
    })
  }

  /**
    * 用户文章维度 旧画像+ 根据日志生成的画像 = 最新用户画像 （最新）
    * @param originalSingleArticleInterestExtend
    * @param userArticleTempNew
    * @param isTotalOrRecent
    * @param autoDecRefresh
    * @return
    */
  def unionOriginAndNewArticleLogInterestsNew(originalSingleArticleInterestExtend:RDD[(String,UserArticleTempNew)],userArticleTempNew:RDD[(String,UserArticleTempNew)],isTotalOrRecent:Boolean,autoDecRefresh:Boolean):RDD[usersNew]={
    //原始用户标签和新生成标签合并
    val value = originalSingleArticleInterestExtend.union(userArticleTempNew).reduceByKey((r1, r2) => {
      var userName = r1.UserName
      var newMap: CustomizedHashMap[String, CustomizedHashMap[String, CustomizedKeyWord]] = r1.prefListExtend

      val value = r2.prefListExtend.keySet().iterator()

      while (value.hasNext) {
        var module_id = value.next()
        val stringToWord = r2.prefListExtend.get(module_id)
        if (newMap.containsKey(module_id)) {
          var oldMap: CustomizedHashMap[String, CustomizedKeyWord] = newMap.get(module_id)
          val iteratorKeywords = stringToWord.keySet().iterator()
          while (iteratorKeywords.hasNext) {

            var keyword = iteratorKeywords.next()

            var keyWordObj = stringToWord.get(keyword)
            if (oldMap.containsKey(keyword)) {
              val keyWordTemp = oldMap.get(keyword)
              keyWordTemp.setWeight(keyWordTemp.getWeight + keyWordObj.getWeight)
              //只有当原始的关键词时间小于新的时间时才更新它
              if (keyWordTemp.getDateTime.toDouble < keyWordObj.getDateTime.toDouble) keyWordTemp.setDateTime(keyWordObj.getDateTime)
              //oldMap.put(keyword,keyWordObj)
            }else{
              oldMap.put(keyword,stringToWord.get(keyword))
            }
          }
        } else {
          newMap.put(module_id, stringToWord)
        }
      }
      UserArticleTempNew(0, UserName, "", newMap, "")
    }).map(u=>{
      if(!isTotalOrRecent){
        //newMap = autoDecRefreshArticleRecentInterests(newMap)

        usersNew(u._2.UserName,"","",0,0,"","","",u._2.prefListExtend.toString,"","","","","","","")
      }else{
        usersNew(u._2.UserName,"","",0,0,"","",u._2.prefListExtend.toString,"","","","","","","","")
      }
    })
    value
  }

  /**
    * 用户关注学科 旧画像+ 根据日志生成的画像 = 最新用户画像 （已弃用）
    * @param origin
    * @param newSubjects
    * @param Type
    * @param dateTime
    * @return
    */
  def unionOriginAndNewUserConceredSubject(origin:RDD[UserConcernedSubjectTemp],newSubjects:RDD[UserConcernedSubjectTemp],Type:String,dateTime:Broadcast[String]):RDD[usersNew]={
    origin.union(newSubjects).groupBy(_.UserName).map(row=>{
      var userName = row._1
      var newMap:CustomizedHashMap[String, CustomizedKeyWord] = new CustomizedHashMap[String, CustomizedKeyWord]
      val iterator = row._2.iterator
      while(iterator.hasNext){
        val userConcernedSubjectTemp = iterator.next()
        if(userConcernedSubjectTemp.prefListExtend.size() >0){
          if(newMap.size() <= 0){
            newMap.putAll(userConcernedSubjectTemp.prefListExtend)
          }else{
            val keyIterator = userConcernedSubjectTemp.prefListExtend.keySet().iterator()
            while(keyIterator.hasNext){
              var key = keyIterator.next()
              var value = userConcernedSubjectTemp.prefListExtend.get(key)
              if(newMap.containsKey(key)){
                var customizedKeyWord = newMap.get(key)
                var newWeight = customizedKeyWord.getWeight + value.getWeight
                customizedKeyWord.setWeight(newWeight)
                if(customizedKeyWord.getDateTime < value.getDateTime) customizedKeyWord.setDateTime(value.getDateTime)
              }else{
                newMap.put(key,value)
              }
            }
          }
        }
      }
      Type match {
        case "UserConceredSubject" => usersNew(userName,"","",0,0,orderByHashMap(newMap,true).toString,"","","","","","","","","",dateTime.value)
        case "UserSubConceredSubject" => usersNew(userName,"","",0,0,"",orderByHashMap(newMap,true).toString,"","","","","","","","",dateTime.value)
        case "TotalRelatedAuthor" => usersNew(userName,"","",0,0,"","","","",orderByHashMap(newMap,true).toString,"","","","","",dateTime.value)
        case "RecentRelatedAuthor" => usersNew(userName,"","",0,0,"","","","","",orderByHashMap(newMap,true).toString,"","","","",dateTime.value)
        case "SearchKeyword" => usersNew(userName,"","",0,0,"","","","","","",orderByHashMap(newMap,true).toString,"","","",dateTime.value)
        case _ => usersNew(userName,"","",0,0,orderByHashMap(newMap,true).toString,"","","","","","","","","",dateTime.value)
      }
    })
  }

  /**
    * 用户关注学科 旧画像+ 根据日志生成的画像 = 最新用户画像 （最新）
    * @param origin
    * @param newSubjects
    * @param Type
    * @return
    */
  def unionOriginAndNewUserConceredSubjectNew(origin:RDD[(String,UserConcernedSubjectTemp)],newSubjects:RDD[(String,UserConcernedSubjectTemp)],Type:String):RDD[usersNew]={
    origin.union(newSubjects).reduceByKey((r1,r2)=>{
      var userName = r1.UserName
      var newMap:CustomizedHashMap[String, CustomizedKeyWord] = r1.prefListExtend

      val iteratorKeywords = r2.prefListExtend.keySet().iterator()
      while(iteratorKeywords.hasNext){
        var keyword = iteratorKeywords.next()
        val customizeKeyword = r2.prefListExtend.get(keyword)
        if(newMap.containsKey(keyword)){
          val oldMap = newMap.get(keyword)
          oldMap.setWeight(oldMap.getWeight + customizeKeyword.getWeight)
          if(oldMap.getDateTime.toDouble < customizeKeyword.getDateTime.toDouble) oldMap.setDateTime(customizeKeyword.getDateTime)
        }else{
          newMap.put(keyword,customizeKeyword)
        }
      }
      UserConcernedSubjectTemp(0, UserName, "", newMap, "")
    }).map(u=>{
      Type match {
        case "UserConceredSubject" => usersNew(u._2.UserName,"","",0,0,orderByHashMap(u._2.prefListExtend,true).toString,"","","","","","","","","","")
        case "UserSubConceredSubject" => usersNew(u._2.UserName,"","",0,0,"",orderByHashMap(u._2.prefListExtend,true).toString,"","","","","","","","","")
        case "TotalRelatedAuthor" => usersNew(u._2.UserName,"","",0,0,"","","","",orderByHashMap(u._2.prefListExtend,true).toString,"","","","","","")
        case "RecentRelatedAuthor" => usersNew(u._2.UserName,"","",0,0,"","","","","",orderByHashMap(u._2.prefListExtend,true).toString,"","","","","")
        case _ => usersNew(u._2.UserName,"","",0,0,orderByHashMap(u._2.prefListExtend,true).toString,"","","","","","","","","","")
      }
    })
  }

  /**
    * 用户画像的各标签按比重排序
    * @param stringToWord
    * @param descending
    * @return
    */
  def orderByHashMap(stringToWord:CustomizedHashMap[String,CustomizedKeyWord],descending:Boolean):CustomizedHashMap[String,CustomizedKeyWord]={
    var newStringToWord:CustomizedHashMap[String,CustomizedKeyWord] = new CustomizedHashMap[String,CustomizedKeyWord]
    if(stringToWord.size()>0){
      val list = new ArrayList[util.Map.Entry[String, CustomizedKeyWord]](stringToWord.entrySet)

      list.sort(new Comparator[Map.Entry[String, CustomizedKeyWord]] {
        override def compare(o1: Map.Entry[String, CustomizedKeyWord], o2: Map.Entry[String, CustomizedKeyWord]): Int = {
          if(o1 == null || o2 == null){
            return 0
          }
          if(descending){
            return o2.getValue.getWeight.compareTo(o1.getValue.getWeight)
          }else{
            return o1.getValue.getWeight.compareTo(o2.getValue.getWeight)
          }
        }
      })
      import scala.collection.JavaConversions._
      for(i<-0 to list.length-1){
        newStringToWord.put(list(i).getKey,list(i).getValue)
      }
    }
    newStringToWord
  }

}
