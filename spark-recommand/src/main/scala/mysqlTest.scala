import org.apache.spark.sql.SparkSession

object mysqlTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate
    
  }
}
