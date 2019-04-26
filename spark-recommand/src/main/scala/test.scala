import com.qianxinyao.analysis.jieba.keyword.TFIDFAnalyzer
import org.apache.spark.sql.{SaveMode, SparkSession}

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate

    val topN = 10
    val tfidfAnalyzer = new TFIDFAnalyzer

    var path = "E:\\test\\journals-recommand-source\\journalbaseinfo_temp.csv"

    import spark.implicits._
    val dataFrame = spark.read.textFile(path).rdd.map(line => {

      val tokens = line.split("::")

      var title = tokens(2)
      var content = ""
      //ansj分词获取tf-idf值（只支持中文）
      //val keywords: util.Iterator[Keyword] = TFIDF.getTFIDE(title, content, 10).iterator()

      //jieba分词获取tf-idf值
      val keywords = TFIDFNEW.getTFIDE(title.replace("\"",""), 10).iterator()

      var temp = new CustomizedHashMap[String,CustomizedHashMap[String,Double]]()

      var map = new CustomizedHashMap[String,Double]()

      temp.put(tokens(5),map)

      while (keywords.hasNext) {
        var keyword = keywords.next()
        val name = keyword.getName
        val score = keyword.getTfidfvalue
        map.put(name,score)
      }

      journalbaseinfo_temp(tokens(0).toInt, tokens(1), tokens(2), tokens(3), tokens(4), tokens(5),tokens(6),temp.toString)
      //news_temp(tokens(0).toInt,"",tokens(2),tokens(3),tokens(4).toInt,tokens(5),temp.toString)
    }).toDF()


    dataFrame.show()
    /**/

    dataFrame.write
      .format("jdbc")
      .option("url", "jdbc:mysql://master02:3306")
      .option("dbtable", "centerDB.journalbaseinfo_temp")
      .option("user", "root")
      .option("password", "root")
      .mode(SaveMode.Overwrite)
      .save()


    println("--------------------数据保存成功......！------------------------")


    spark.stop()


  }
  case class journalbaseinfo_temp(id:BigInt, PYKM:String, C_Name:String, CBD:String, ZJ_Code:String, ZT_Code:String, CreateOn:String,Keywords:String)

  case class news_temp(id:BigInt, content:String, news_time:String, title:String, module_id:Int, url:String, keywords:String)
}
