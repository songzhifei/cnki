
import com.redislabs.provider.redis._
import org.apache.spark.sql.SparkSession

object testReadAndWrite {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("redis-df")
      //.master("local[*]")
      .config("spark.redis.host", "master02")
      .config("spark.redis.port", "6379")
      //.config("spark.redis.db","3")
      .getOrCreate()

    val sc = spark.sparkContext

    //sc.fromRedisKV("Mozilla/5.0+*").collect().foreach{println}

    sc.fromRedisKeyPattern("Mozilla/5.0+*", 5).collect().foreach{println}

    //sc.toRedisKV()

    spark.stop()
  }
}
