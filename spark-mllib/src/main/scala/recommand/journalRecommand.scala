package recommand

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import scala.collection.mutable

object journalRecommand {
  case class Rating(userId: Int, productId: Int, rating: Float)
  case class recsObj(userId:Int,userName:String,productId:Int,productName:String,rating:Float)
  def parseRating(str: String): Rating = {
    val fields = str.split(",")
    assert(fields.size >= 3)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
  }
  def parseMapping(str:String)={
    val fields = str.split(",")
    assert(fields.length >= 2)
    (fields(0).toInt, fields(1))
  }
  def loadModel(path:String)={
    ALSModel.load(path)
  }
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("journalRecommand")
      .getOrCreate()

    val sc = spark.sparkContext

    val basePath = args(0)

    var modelPath = basePath + "model/"

    import spark.implicits._

    val userMap = spark.read.textFile(basePath+args(1)).rdd.map(parseMapping).collectAsMap()

    val productMap = spark.read.textFile(basePath+args(2)).rdd.map(parseMapping).collectAsMap()

    val userBroadCast = sc.broadcast(userMap)

    val proBroadCast = sc.broadcast(productMap)

    var model:ALSModel = null
    //判断model的路径是否存在
    val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    def testDirExist(path: String): Boolean = {
      val p = new Path(path)
      hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isDirectory
    }
    if(testDirExist(modelPath)){
      model = loadModel(modelPath)
    }

    if(model == null){
      // $example on$
      val ratings = spark.read.textFile(basePath+args(3))
        .map(parseRating)
        .toDF()
      val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
      // Build the recommendation model using ALS on the training data
      val als = new ALS()
        .setMaxIter(5)
        .setRegParam(0.01)
        .setUserCol("userId")
        .setItemCol("productId")
        .setRatingCol("rating")
      model = als.fit(training)

      model.write.overwrite().save(modelPath)

      // Evaluate the model by computing the RMSE on the test data
      // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
      model.setColdStartStrategy("drop")
      val predictions = model.transform(test)

      val evaluator = new RegressionEvaluator()
        .setMetricName("rmse")
        .setLabelCol("rating")
        .setPredictionCol("prediction")
      val rmse = evaluator.evaluate(predictions)
      println(s"Root-mean-square error = $rmse")
    }

    // Generate top 10 movie recommendations for each user
    val userRecs = model.recommendForAllUsers(10)
    // Generate top 10 user recommendations for each movie
    //val movieRecs = model.recommendForAllItems(10)


    val dataFrame: DataFrame = userRecs.rdd.flatMap(row => {
      val user_id = row.getInt(0)
      val recs = row.getAs[mutable.WrappedArray[Row]](1)
      recs.map(rec => {
        val item_id = rec.getInt(0)
        val item_rating = rec.getFloat(1)
        val userName = userBroadCast.value.get(user_id).getOrElse("")
        val productName = proBroadCast.value.get(item_id).getOrElse("")
        recsObj(user_id, userName, item_id, productName, item_rating)
      })
    }).toDF()
    dataFrame.show(10,false)
    dataFrame.repartition(1).write.mode(SaveMode.Overwrite).format("csv").save(basePath+"/output")
    println("------------------------用户推荐结果集保存成功，路径为："+ basePath + "/output---------------------------------")
    spark.stop()
  }

}
