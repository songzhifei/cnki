import java.util.UUID

import CommonFuction._
import CommonObj.{Log_Temp, users}
import org.apache.hadoop.hbase.util.Hash
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object test_func {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("readCSV").getOrCreate
    var timeStr = Md5Util.md5("1563377094476")

    println(UUID.randomUUID())
    spark.stop()
  }
}
