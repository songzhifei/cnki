import CommonFuction.{SEP, formatUsers, getNowStr}
import CommonObj.users
import org.apache.spark.sql.{SaveMode, SparkSession}

object formatUserInfo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("formatUserInfo").getOrCreate

    import spark.implicits._
    val logTime = spark.sparkContext.broadcast(getNowStr())
    //1.1. 获取用户画像数据（格式化用户兴趣标签数据）getYesterday() +
    var userList = spark.read.textFile(args(0)).rdd.map(line=>{
      val strings = line.split("&")
      strings(1)
    }).distinct().map(str=>{
      users(0,str,"","",0,0,"{}","{}","{}","{}","{}","{}","{}",logTime.value)
    }).toDF()

    userList.repartition(1)
      .write
      .format("csv")
      .option("sep",SEP)
      .mode(SaveMode.Overwrite)
      .save(args(1))
  }
}
