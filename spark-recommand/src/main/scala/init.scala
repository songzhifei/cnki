import common._
import org.apache.spark.sql.{SaveMode, SparkSession}


/*
* 初始化用户画像
* */
object init {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("recommand-system").getOrCreate
    import spark.implicits._
    val frame = spark.read.textFile(args(0)).map(line => {
      val tokens = line.split("::")
      users(tokens(0).toInt, tokens(1), "", "", 0, 0, "", "", "", "", "", "", "", "")
    }).toDF()
    println("----------------------用户画像正在保存......--------------------------")
    /*
    userDataFrame.write
      .format("jdbc")
      .option("url", "jdbc:mysql://master02:3306")
      .option("dbtable", "centerDB.users_temp")
      .option("user", "root")
      .option("password", "root")
      .mode(SaveMode.Overwrite)
      .save()
    * */


    frame.repartition(1)
      .write
      .format("csv")
      //.option("escape","")
      .option("sep",SEP)
      //.option("header",true)
      .mode(SaveMode.Overwrite)
      .save(args(1))
    println("----------------------用户画像更新成功--------------------------")

    spark.stop()

  }
}
