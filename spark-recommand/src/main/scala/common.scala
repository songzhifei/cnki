import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{ArrayList, Date}

import org.apache.spark.broadcast.Broadcast
import org.codehaus.jackson.JsonParseException
import org.codehaus.jackson.`type`.TypeReference
import org.codehaus.jackson.map.{JsonMappingException, ObjectMapper}

import scala.collection.mutable.ArrayBuffer

object common {

  var objectMapper:ObjectMapper = null

  case class UserTemp(id:Long,username:String, prefList:String,prefListExtend:CustomizedHashMap[String, CustomizedHashMap[String, Double]],latest_log_time:String)

  case class NewsLog_Temp(username:String,view_time:String,title:String,content:String,module_id:String,map:CustomizedHashMap[String, Double])

  case class users(id:Long,name:String, prefList:String,latest_log_time:String)

  case class UserPortrait(UserID:BigInt,UserName:String,LawOfworkAndRest:Int,Area:String,Age:Int,Gender:Int,SingleArticleInterest:String)

  case class NewsTemp(id:Long, content:String, news_time:String, title:String, module_id:Int, url:String, keywords:CustomizedHashMap[String, Double])

  case class recommendations(user_id:BigInt,news_id:BigInt,score:Double)

  def formatUserViewLogs(line:String): NewsLog_Temp ={
    val tokens = line.split("::")

    //val date1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(tokens(1))

    val map = jsonPrefListtoMap(tokens(4))

    NewsLog_Temp(tokens(0),tokens(1),tokens(2),"",tokens(3),map.get(tokens(3)))
  }

  def autoDecRefresh(user:UserTemp): UserTemp ={

    //用于删除喜好值过低的关键词
    val keywordToDelete = new ArrayList[String]

    val map =user.prefListExtend

    var baseAttenuationCoefficient = 0.9

    var times = 1L

    if(!user.latest_log_time.isEmpty)
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

    UserTemp(user.id,user.username,user.prefList,newMap,user.latest_log_time)

  }

  def getUserPortrait(user:UserTemp,newsBroadCast:Broadcast[collection.Map[String, Array[NewsLog_Temp]]],logTime:Broadcast[String]): users ={
    val newsList: Array[NewsLog_Temp] = newsBroadCast.value.get(user.username).getOrElse(new Array[NewsLog_Temp](0))
    println("处理前的rateMap：" + user.prefListExtend.toString)
    if (newsList.length >0) {
      //1.3. 根据用户画像和浏览历史更新各个用户的用户兴趣标签
      for(news <- newsList){
        var rateMap: CustomizedHashMap[String, Double] = user.prefListExtend.get(news.module_id)
        //println("原始rateMap：" + rateMap)
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

        //println("处理后的rateMap：" + user.prefListExtend.get(news.module_id))
      }
    }
    println("处理后的rateMap：" + user.prefListExtend.toString)

    users(user.id,user.username,user.prefListExtend.toString,logTime.value)
  }

  def jsonPrefListtoMap (srcJson: String): CustomizedHashMap[String, CustomizedHashMap[String, Double]] = {

    if(objectMapper == null) {
      objectMapper = new ObjectMapper
      println(1)
    }
    var result = new CustomizedHashMap[String, CustomizedHashMap[String, Double]]()
    try{
      println(srcJson)
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

  def compare(t1:(Long,Long,Double),t2:(Long,Long,Double)): Boolean ={
    t1._3 > t2._3
  }

  def formatUsers(line:String):UserTemp={
    val tokens = line.split("::")
    UserTemp(tokens(0).toLong,tokens(1),"{}",jsonPrefListtoMap("{}"),tokens(3))
  }

  def formatNews(line:String): NewsTemp ={

    val tokens = line.split("::")

    var title = tokens(3)
    var content = ""
    var map:CustomizedHashMap[String,Double] = jsonPrefListtoMap(tokens(6)).get(tokens(4).toInt)

    //println(jsonPrefListtoMap(tokens(6)),map,tokens(4).toInt,title,tokens(6))

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
    NewsTemp(tokens(0).toLong, "", tokens(2), title, tokens(4).toInt, tokens(5), map)
  }

  def formatRecommandTuple(user:UserTemp,newsBroadCast:Broadcast[Array[NewsTemp]]): ArrayBuffer[(Long, Long, Double)] ={
    var tempMatchArr = new ArrayBuffer[(Long, Long, Double)]()
    var ite = newsBroadCast.value.iterator
    while (ite.hasNext) {
      val news = ite.next
      val newsId = news.id
      val moduleId = news.module_id

      if (null != user.prefListExtend.get(moduleId)) {
        val tuple: (Long, Long, Double) = (user.id, newsId, getMatchValue(user.prefListExtend.get(moduleId), news.keywords))
        tempMatchArr += tuple
      }
    }
    // 去除匹配值为0的项目,并排序
    var sortedTuples: ArrayBuffer[(Long, Long, Double)] = tempMatchArr.filter(tuple => tuple._3 > 0).sortWith(compare)

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

  def getNowStr():String={
    var now =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
    now
  }
}
