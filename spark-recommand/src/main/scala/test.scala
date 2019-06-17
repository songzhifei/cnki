import com.qianxinyao.analysis.jieba.keyword.TFIDFAnalyzer
import org.apache.spark.sql.{SaveMode, SparkSession}

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate

    val topN = 10
    val tfidfAnalyzer = new TFIDFAnalyzer

    var path = "E:\\test\\recommend-system\\journal\\journalbaseinfo_new.csv"

    import spark.implicits._
    val dataFrame = spark.read.textFile(path).rdd.map(line => {

      val tokens = line.split("::")

      var title = tokens(2).replace("&","")

      var content = tokens(3).replace("&","")
      //ansj分词获取tf-idf值（只支持中文）
      //val keywords: util.Iterator[Keyword] = TFIDF.getTFIDE(title, content, 10).iterator()

      //jieba分词获取tf-idf值
      //val keywords = TFIDFNEW.getTFIDE(title.replace("\"",""), 10).iterator()

      val keywordsNew = TFIDFNEW.getTFIDE(content.replace("\"","").replace(" ","").replace(";",""), 10).iterator()

      //var temp = new CustomizedHashMap[String,CustomizedHashMap[String,Double]]()

      var tempNew = new CustomizedHashMap[String,CustomizedHashMap[String,Double]]()

      //var map = new CustomizedHashMap[String,Double]()

      var mapNew = new CustomizedHashMap[String,Double]()

      tempNew.put(tokens(6),mapNew)

      //temp.put(tokens(6),map)

      while (keywordsNew.hasNext) {
        var keyword = keywordsNew.next()
        val name = keyword.getName
        val score = keyword.getTfidfvalue
        mapNew.put(name,score)
      }
      var t = tempNew.toString
      println(t)
      journalbaseinfo_temp(tokens(0).toInt, tokens(1), title, content, tokens(4), tokens(5),tokens(6),tokens(7),t)
    }).toDF()


    dataFrame.show()
    /*

    dataFrame.write
      .format("jdbc")
      .option("url", "jdbc:mysql://master02:3306")
      .option("dbtable", "centerDB.journalbaseinfo_temp")
      .option("user", "root")
      .option("password", "root")
      .mode(SaveMode.Overwrite)
      .save()

    */

    dataFrame.repartition(1)
      .write
      .format("csv")
      //.option("escape","")
      .option("sep","&")
      //.option("header",true)
      .mode(SaveMode.Overwrite)
      .save("E:/test/recommend-system/journal/journalbaseinfo_temp")
    println("--------------------数据保存成功......！------------------------")


    spark.stop()


  }
  case class journalbaseinfo_temp(id:BigInt, PYKM:String, C_Name:String,CYKM:String, CBD:String, ZJ_Code:String, ZT_Code:String, CreateOn:String,Keywords:String)

  case class news_temp(id:BigInt, content:String, news_time:String, title:String, module_id:Int, url:String, keywords:String)

  case class UserTemp(UserID:BigInt,UserName:String, prefList:String,prefListExtend:CustomizedHashMap[String, CustomizedHashMap[String, Double]],latest_log_time:String)
}
