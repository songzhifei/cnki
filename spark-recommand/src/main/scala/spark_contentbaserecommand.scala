import CommonFuction.formatNews
import org.apache.spark.ml.feature.{HashingTF, IDF, Word2Vec}
import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.linalg.{SparseVector => SV,DenseVector =>DV}

object spark_contentbaserecommand {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("spark_contentbaserecommand")
      .getOrCreate()
    import spark.implicits._
    var wordsData = spark.read.textFile(args(0)).rdd.map(formatNews).map(news=>{
      val keywords = news.keywords
      val iterator = keywords.keySet().iterator()
      val stringArr = new ArrayBuffer[String]()
      while(iterator.hasNext){
        var keyword = iterator.next()
        stringArr += keyword
      }
      (news.id,stringArr.toArray)
    }).filter(_._2.length > 0).toDF("label", "words")

    wordsData.show(20)

    /*

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)


    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    val tfidf = rescaledData.select("label", "features")

    val b_tfidf = spark.sparkContext.broadcast(tfidf.collect())

    val dataFrame = tfidf.flatMap(row => {
      val id1 = row.getAs[Long](0)
      val idf1 = row.get(1)
      val idfs = b_tfidf.value.filter(_.getAs[Long](0) != id1)
      val sv1 = idf1.asInstanceOf[SV]

      import breeze.linalg._
      val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)

      val tuples: Array[(Long, Long, Double)] = idfs.map(row2 => {
        val id2 = row2.getAs[Long](0)
        val idf2 = row2.get(1)
        val sv2 = idf2.asInstanceOf[SV]
        val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
        val cosSim = bsv1.dot(bsv2).asInstanceOf[Double] / ((norm(bsv1) * norm(bsv2)) + 1)
        (id1, id2, cosSim)
      })
      val newTuples = tuples.sortWith(userDefindSort).take(10)
      newTuples
    }).map(data=>{
      recommendations(data._1,data._2,data._3)
    }).toDF()
*/
    /*  */
        // Learn a mapping from words to Vectors.
        val word2Vec = new Word2Vec()
          .setInputCol("words")
          .setOutputCol("result")
          .setVectorSize(3)
          .setMinCount(0)

        val model = word2Vec.fit(wordsData)

        val result = model.transform(wordsData)

        //result.show(20)

        //result.printSchema()

        val featureDataFrame = result.select("label","result")

        val b_featureDataFrame = spark.sparkContext.broadcast(featureDataFrame.collect())

        val dataFrame = featureDataFrame.flatMap(row => {
          val id1 = row.getAs[Long](0)
          val idf1 = row.get(1)
          //println(id1,idf1.toString)
          val idfs = b_featureDataFrame.value.filter(_.getAs[Long](0) != id1)
          val sv1 = idf1.asInstanceOf[DV]
          //println(sv1)
          import breeze.linalg._
          val bsv1 = new DenseVector[Double](sv1.values)

      val tuples: Array[(Long, Long, Double)] = idfs.map(row2 => {
        val id2 = row2.getAs[Long](0)
        val idf2 = row2.get(1)
        //println(idf2)
        val sv2 = idf2.asInstanceOf[DV]
        val bsv2 = new DenseVector[Double](sv2.values)
        val cosSim = bsv1.dot(bsv2).asInstanceOf[Double] / ((norm(bsv1) * norm(bsv2)) + 1)
        (id1, id2, cosSim)
      })
      val newTuples = tuples.sortWith(userDefindSort).take(10)
      newTuples
    }).map(data=>{
      recommendations(data._1,data._2,data._3)
    }).toDF()

    dataFrame.repartition(1).write.mode(SaveMode.Overwrite).format("csv").save(args(1))

    spark.stop()
  }
  def userDefindSort(t1:(Long, Long, Double),t2:(Long, Long, Double)): Boolean ={
    return t1._3.compareTo(t2._3) >0
  }

  case class recommendations(i1:Long,i2:Long,score:Double)
}
