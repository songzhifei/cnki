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
    val newsList = spark.read.textFile("E:/test/recommend-system/journal/journalbaseinfo_temp/").rdd.map(formatNews).collect()

    //jsonPrefListtoMap("{\"H131\":{\"TheTwenty\":0.2989,\"世纪\":0.117,\"FirstCentury\":0.2989,\"21\":0.2989}}")
    //jsonPrefListtoMap("{\"J152\":{\\\"OrientalEnterpriseCulture\\\":0.2989,\\\"企业\\\":0.1055,\\\"文化\\\":0.1272,\\\"东方\\\":0.1499}}")

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
