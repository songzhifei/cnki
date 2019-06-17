import CommonFuction.{SEP, jsonPrefListtoMap}
import CommonObj._
import org.apache.spark.sql.{SaveMode, SparkSession}

object initBookBaseInfo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("initBookBaseInfo").getOrCreate

    import spark.implicits._
    val  bookList= spark
      .read
      .textFile("E:/test/recommend-system/tb_book_cnki.csv")
      .rdd
      .map(line=>{
        val tokens = line.split("&&")

        if(tokens.size == 5){
          var id = tokens(2)

          var title = tokens(1)
          var content = tokens(3).replace("NULL","").replace("&nbsp;","").replace("<p>","").replace("</p>","").replace("&","").replace("<br/>","")
          var class_code = tokens(4)
          println(title,content)

          var keys = if(!content.isEmpty) title+content else title

          println(keys)

          val keywords = TFIDFNEW.getTFIDE(keys, 10).iterator()

          val map = new CustomizedHashMap[String,Double]()

          while (keywords.hasNext) {
            var keyword = keywords.next()
            val name = keyword.getName
            val score = keyword.getTfidfvalue
            map.put(name,score)
          }
          BookBaseInfo(id,title,content,class_code,map.toString)
        }else{
          BookBaseInfo("","","","","")
        }

      }).filter(_.id!="").toDF()

    bookList
      .repartition(1)
      .write
      .format("csv")
      .option("sep",SEP)
      .mode(SaveMode.Overwrite)
      .save("E:/test/recommend-system/bookbaseinfo/")

    println("图书基本数据更新成功,更新数量：")

    spark.stop()
  }
}
