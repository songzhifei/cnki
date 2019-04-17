package wordstag

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object CountVectorizerIDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("wordstagtest")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = spark.sqlContext

    val path = "E:\\test\\training-news\\"
    val rdd = sc.wholeTextFiles(path) //读取目录下所有文件:[(filename1:content1),(filename2:context2)]


    //val stopWords = List("／","·", "；","＂","．","％","：","］","［","－");
    //过滤停用词
    import scala.collection.JavaConversions._
    val stopWords = sc.textFile("E:\\test\\stop-words.txt").collect().toSeq //构建停词
    val filter = new StopRecognition().insertStopWords(stopWords) //过滤停词

    val splitWordRdd = rdd.map(file => { //使用中文分词器将内容分词：[(filename1:w1 w3 w3...),(filename2:w1 w2 w3...)]
      val str = ToAnalysis.parse(file._2).recognition(filter).toStringWithOutNature(" ")
      (file._1, str.split(" "))
    })

    val pplDf = ssc.createDataFrame(splitWordRdd).toDF("fileName", "content");

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("content")
      .setOutputCol("feature")
      .setVocabSize(10000) //向量长度
      .setMinDF(2) //词汇出现次数必须大于等于2
      .fit(pplDf)
    val cvDf = cvModel.transform(pplDf)

    val idf = new IDF().setInputCol("feature").setOutputCol("features")
    val idfModel = idf.fit(cvDf)
    val idfDf = idfModel.transform(cvDf).drop("content").drop("feature")
    idfDf.show(false)

    val voc= cvModel.vocabulary

    voc.foreach{println}
    val getKeyWordsFun = udf((fea:Vector)=>{
      var arrW = ArrayBuffer[String]()
      var arrV = ArrayBuffer[Double]()
      fea.foreachActive((index:Int,value:Double)=>{
        arrW += voc(index)
        arrV += value
      })
      (arrW zip arrV).toList.sortBy(-_._2).take(25).toMap.map(_._1).toArray
    })

    val keyWordsDf = idfDf.withColumn("keywords",getKeyWordsFun(col("features"))).drop("features")

    keyWordsDf.show(false)

    splitWordRdd.flatMap(_._2).collect().foreach{println}

    spark.stop()

  }
}
