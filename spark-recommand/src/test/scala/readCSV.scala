import java.util.Date

import org.apache.spark.sql.{SaveMode, SparkSession}
import CommonFuction.{objectMapper, _}
import CommonObj.{UserLogObj, usersNew}
import org.codehaus.jackson.`type`.TypeReference
import org.codehaus.jackson.map.ObjectMapper
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.util.parsing.json.JSON

object readCSV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("readCSV").getOrCreate

    /*
    import spark.implicits._
    val logTime = spark.sparkContext.broadcast(getNowStr())
    //1.1. 获取用户画像数据（格式化用户兴趣标签数据）getYesterday() +
    var userList = spark.read.textFile("E:/test/recommend-system/journal/userlog/2019-07-13").rdd.map(line=>formatUsersNew(line,logTime)).filter(user=> !user.UserName.isEmpty)


    userList.map(user=>{

      jsonConcernedSubjectListToMap(user.TotalRelatedAuthor).toString
    }).collect().foreach{println}
    */
    //spark.read.parquet("E:/test/recommend-system/journal/userlog/2019-07-13").


      spark.stop()
  }
}
