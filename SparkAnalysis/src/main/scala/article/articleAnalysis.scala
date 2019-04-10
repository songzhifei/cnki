package article

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object articleAnalysis {

  private var dataTypeARR:Array[String] = Array("会议","博士","期刊","硕士","报纸","其他")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      //.master("local[*]")
      .appName("test")
      .getOrCreate()



    val year = if(args.length>2) args(2).toInt else 2018

    val limit = if(args.length>3) args(3).toInt else 100

    val sc = spark.sparkContext

    val dataTypeBroadcast = sc.broadcast(dataTypeARR)

    val linesRDD = sc.textFile(args(0))

    import spark.implicits._
    val titleDF = sc.textFile(args(1))
      .map(line=>{
        val strings = line.split("\t")
        if(strings.length>=2){
          articleBuyBrowser(strings(0),strings(1),"",0,0,0)
        }else{
          articleBuyBrowser("","","",0,0,0)
        }
      }).groupBy(f=>f.FileName)
      .flatMap(_._2.toList.take(1))
      .distinct()
      .filter(f=>f.FileName.nonEmpty).toDF

    titleDF.persist(StorageLevel.MEMORY_AND_DISK)

    val articleRDD: RDD[((String, String), Int)] = linesRDD.map(dataETL)
      .filter(_._2 !=0)
      .reduceByKey(_+_)
      .groupBy(_._1._2)
      .flatMap(_._2.toList)


    val dataFrame = articleRDD.map(row => {
      articleBuyBrowser(row._1._1,"", row._1._2, row._2,0,year)
    }).toDF

    var dataFrameNew: Dataset[Row] = dataFrame.limit(0)

    for(i <- dataTypeBroadcast.value){
      dataFrameNew = dataFrameNew.union(dataFrame.where("dataType = '" + i +"'").orderBy(dataFrame("buyCount").desc).limit(limit))
    }

    dataFrameNew.persist(StorageLevel.MEMORY_AND_DISK)

    val resultDF = dataFrameNew
      .join(titleDF,Seq("fileName"),joinType="inner")
      .select(dataFrameNew("FileName"),titleDF("Title"),dataFrameNew("DataType"),dataFrameNew("BuyCount"),dataFrameNew("BrowseCount"),dataFrameNew("Year"))

    resultDF.show(20)

    println("--------------------------------数据统计成功，开始写入mysql.....--------------------------------")

    // Saving data to a JDBC source
    resultDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://master02:3306")
      .option("dbtable", "articlebuylog.articleBuyBrowseRank")
      .option("user", "root")
      .option("password", "root")
      .mode(SaveMode.Append)
      .save()

    println("--------------------------------数据成功写入mysql--------------------------------")


    spark.stop()
  }

  def formateDataType(dataType:String):String={
    val value =dataType match {
      case "CPFD"|"中国会议"|"会议论文"=>"会议"
      case "CDFD"|"博士论文"|"博士"=>"博士"
      case "期刊"|"CJFD"=>"期刊"
      case "硕士论文"|"CMFD"|"硕士"=>"硕士"
      case "报纸文献"|"CCND"|"报纸"=>"报纸"
      case _=>"其他"
    }
    value
  }

  def dataETL(str:String)={
    val tokens = str.split("\t")
    //assert(tokens.length >= 3)
    if(tokens.length >=2){
      val fileName = tokens(0)
      val dataType = tokens(1)
      ((fileName,formateDataType(dataType)),1)
    }else{
      ((null,null),0)
    }

  }

  case class articleBuyBrowser(FileName:String,Title:String,DataType:String,BuyCount:BigInt,BrowseCount:BigInt,Year:Int)
}