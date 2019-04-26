package UserPortrait

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object portrait_tag {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("portrait_tag")
      .getOrCreate()

    var maxTagsLength = if(args.length>2) args(2).toInt else 10

    val linesRDD = spark.read.textFile(args(0)).rdd

    val dataETL = linesRDD.map(line => {
      val tokens = line.split("\t")
      if (tokens.length >= 7) {
        val username = tokens(0)
        val buyCount = tokens(3).toInt
        val category = tokens(6)
        ((username, category), buyCount)
      } else {
        (("", ""), 0)
      }
    }).filter(_._2 >= 0).reduceByKey(_ + _)

    //dataETL.groupBy(_._1._1).collect().foreach{println}

    import spark.implicits._
    val dataFrame = dataETL.groupBy(_._1._1).map { case row => {
      var tags = ""
      //按照购买量从大到小排序
      var list = row._2.toList.sortWith((s,t)=>s._2.compareTo(t._2)>0)
      if(list.length > maxTagsLength) list = list.take(maxTagsLength)

      for (tuple <- list) {
        tags += tuple._1._2 + ";"
      }
      if (tags.endsWith(";")) {
        tags = tags.substring(0, tags.length - 1)
      }
      UserPortrait(row._1, tags)
    }
    }.toDF()

    //dataFrame.repartition(1).write.mode(SaveMode.Overwrite).format("csv").save(args(1))

    dataFrame.write
      .format("jdbc")
      .option("url", "jdbc:mysql://master02:3306")
      .option("dbtable", "`spark_test`.`UserPortrait`")
      .option("user", "root")
      .option("password", "root")
      .mode(SaveMode.Append)
      .save()

    println("----------------------用户画像数据保存成功，位置：`spark_test`.`UserPortrait`-------------------------------")
    //temp.collect().foreach{println}

    spark.stop()
  }
  case class UserPortrait(UserName:String,JournalsInterests:String)
}
