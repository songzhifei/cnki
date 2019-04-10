package journal

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{SaveMode, SparkSession}

object journalAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("journalAnalysis")
      .getOrCreate()


    val createonBroadCast = spark.sparkContext.broadcast(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))

    import spark.implicits._
    val dataFrame = spark.read.textFile(args(0)).rdd.map(dataETL).filter(f => f._1._2 != "\\N" && f._1._3 != "\\N").reduceByKey(_ + _).map(f => {
      journalsbuybrowse(f._1._1, f._1._2, f._1._3, "", f._1._4.toInt, f._1._5.toInt, f._1._6, 0, f._2.toInt, createonBroadCast.value)
    }).toDF()

    println("--------------------------------数据统计成功，开始写入mysql.....--------------------------------")

    dataFrame.write
      .format("jdbc")
      .option("url", "jdbc:mysql://master02:3306")
      .option("dbtable", "cnkianalysis_db.journalsbuybrowse")
      .option("user", "root")
      .option("password", "root")
      .mode(SaveMode.Append)
      .save()

    println("--------------------------------数据成功写入mysql--------------------------------")

    println("--------------------------------数据开始写入hdfs--------------------------------")

    dataFrame.repartition(1).write.mode(SaveMode.Overwrite).format("csv").save(args(1))

    println("--------------------------------数据成功写入hdfs--------------------------------")

    spark.stop()

  }
  def dataETL(str:String)={
    val tokens = str.split("\t")
    assert(tokens.length >= 5)
    val code = tokens(0)
    val date = tokens(2).split(" ")(0)
    val dateArr = date.split("-")
    assert(dateArr.length >= 3)
    val albumcategory = tokens(3)
    val subjectcategory = tokens(4)
    ((code,albumcategory,subjectcategory,dateArr(0),dateArr(1),date),formatTimes(tokens(1)))
  }

  def formatTimes(str:String)={
    var res = str.toInt
    res = if(res == 2) 12 else res
    res
  }
  case class journalsbuybrowse(code:String,albumcategory:String,subjectcategory:String,subjectsubcategory:String,year:Int,month:Int,datadate:String,browsecount:BigInt,buycount:BigInt,createon:String)
}
