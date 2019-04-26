import java.io.IOException

import org.apache.spark.sql.SparkSession
import org.codehaus.jackson.JsonParseException
import org.codehaus.jackson.`type`.TypeReference
import org.codehaus.jackson.map.{JsonMappingException, ObjectMapper}
import common._

object json2map {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("json2map").getOrCreate

    //2.2 获取所有待推荐的新闻列表（格式化所有新闻对应的关键词及关键词的权重）
    //val newsList = spark.read.textFile("E:/test/recommend-system/journal/user_temp.csv").rdd.map(formatUsers).collect()

    jsonPrefListtoMap("{\"H129\":{\"作文\":0.3092,\"小学版\":0.3985,\"创新\":0.1914},\"F083\":{\"喜剧\":0.2758,\"半月\":0.2607,\"世界\":0.1456},\"H130\":{\"作文\":0.4638,\"快乐\":0.3713}}")

    //newsList.foreach{println}
  }
  def jsonPrefListtoMap (srcJson: String): CustomizedHashMap[String, CustomizedHashMap[String, Double]] = {
    val objectMapper = new ObjectMapper
    var map = new CustomizedHashMap[String, CustomizedHashMap[String, Double]]
    try
      map = objectMapper.readValue(srcJson, new TypeReference[CustomizedHashMap[String, CustomizedHashMap[String, Double]]]() {})
    catch {
      case e: JsonParseException =>
        e.printStackTrace()
      case e: JsonMappingException =>
        // TODO Auto-generated catch block
        e.printStackTrace()
      case e: IOException =>
        e.printStackTrace()
    }
    return map
  }
}
