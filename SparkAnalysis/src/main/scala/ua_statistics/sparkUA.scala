package ua_statistics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object sparkUA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("sparkUA")
      .getOrCreate()
    val lines = spark.read.textFile(args(0)).rdd


    import spark.implicits._
    val dataFrame = lines.map(f => {
      val line = f.split(" ")
      if (line.length >= 15) {
        Some(((line(0), line(9)), 1))
        ((line(0), line(9)), 1)
      } else {
        (("", ""), 1)
      }
    }).filter(row => !(row._1._1.isEmpty || row._1._1.contains("Fields") || row._1._2 == "-")).reduceByKey(_ + _).map(row => {
      UAobc(row._1._1, row._1._2, row._2.toInt)
    }).toDF()

    dataFrame.repartition(1).orderBy(dataFrame("count").desc).write.partitionBy("date").mode(SaveMode.Overwrite).format("csv").save(args(1))

    spark.stop()
  }
  case class UAobc(date:String,userAgent:String,count:Int)
}
