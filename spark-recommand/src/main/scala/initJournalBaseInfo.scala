import common._
import org.apache.spark.sql.{SaveMode, SparkSession}

object initJournalBaseInfo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate

    import spark.implicits._
    val newsList = spark
      .read
      .textFile("E:/test/journals-recommand-source/part-00000-15a4bef0-00c3-452d-bf4c-5cc4310a35f1-c000.csv")
      .rdd
      .map(line=>{
        val tokens = line.split(SEP)

        var title = tokens(3)
        var content = ""
        var jsonStr = tokens(8).substring(1,tokens(8).length-1).replace("\\","")
        var map:CustomizedHashMap[String, CustomizedHashMap[String, Double]] = jsonPrefListtoMap(jsonStr)
        //println(tokens(1),tokens(6),map.toString)
        JournalBaseTemp(tokens(0).toLong,tokens(1), "",tokens(7), title, tokens(6),map.toString)
      })
      .toDF()

    val newsRDD = newsList.rdd.map(row=>{
      row.getAs[String]("PYKM")
    }).collect()

    val newsBroadCast = spark.sparkContext.broadcast(newsList)

    val newsRDDBroadCast = spark.sparkContext.broadcast(newsRDD)

    val newsTemp = spark.read.textFile("E:/test/journals-recommand-source/mysqldata_2019_05_20_16_02_32.sql").rdd
      .map(line=>{
      val tokens = line.split(",")
      if(tokens.length == 6){
        JournalBaseTemp(1L,tokens(0).replace("\"",""),"","2019-04-28 10:41:46",tokens(5).replace("\"",""),tokens(4).replace("\"",""),"")
      }else{
        JournalBaseTemp(0L,"","","","","","")
      }

    }).filter(jour=>jour.id != 0L)
      .filter(jour=>{
        val bool = if(newsRDDBroadCast.value.contains(jour.PYKM)) false else true
        bool
      }).map(jour=>{

      val keywords = TFIDFNEW.getTFIDE(jour.title, 10).iterator()

      val map = new CustomizedHashMap[String,Double]()

      while (keywords.hasNext) {
        var keyword = keywords.next()
        val name = keyword.getName
        val score = keyword.getTfidfvalue
        map.put(name,score)
      }

      JournalBaseTemp(jour.id,jour.PYKM,jour.content,jour.news_time,jour.title,jour.module_id,map.toString)

    }).toDF()

    newsBroadCast.value
      .union(newsTemp)
      .repartition(1)
      .write
      .format("csv")
      //.option("escape","")
      .option("sep",SEP)
      //.option("header",true)
      .mode(SaveMode.Overwrite)
      .save("E:/test/recommend-system/journal/journalbaseinfo_temp/")

    var size = newsTemp.collect().length

    println("期刊基本数据更新成功,更新数量："+size)

    spark.stop()
  }
}
