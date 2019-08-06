import CommonFuction.{SEP}
import CommonObj._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 根据用户信息初始化用户画像（无用）
  */
object initUserPortrait {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("initUserPortrait").getOrCreate

    val userOrigin = spark.read.textFile(args(0)).rdd.map(line=>{
      val strings = line.split("\t")
      if(strings.length == 2)
        strings(1)
      else
        ""
    })
    val biankeUser = spark.read.textFile(args(1)).rdd
    val wapUser = spark.read.textFile(args(2)).rdd

    val tempUser = userOrigin.union(biankeUser).union(wapUser).filter(str=> !str.isEmpty && str.trim!="").distinct().zipWithIndex()



    import spark.implicits._
    val frame = tempUser.map(str => {
      users(str._2, str._1, "", "", 0, 0, "", "", "", "", "", "", "", "")
    }).toDF()

    frame
      //.union(newUsers)
      .repartition(1)
      .write
      .format("csv")
      //.option("escape","")
      .option("sep",SEP)
      //.option("header",true)
      .mode(SaveMode.Overwrite)
      .save("E:/test/recommend-system/journal/UserPortraitOutputTemp")

    spark.stop()
  }
}
