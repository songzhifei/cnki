import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Durability, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

object SaveToHbase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .config("hive.metastore.uris","thrift://master01:9083")//SPARK读取hive中的元数据必须配置metastore地址
      .config("spark.sql.warehouse.dir", "hdfs://nameservice1/user/hive/warehouse")//配置hive的数据存储目录
      .appName(SaveToHbase.getClass.getSimpleName)
      .enableHiveSupport()
      .getOrCreate

    //定义HBase的配置，保证wetag已经创建
    val conf = HBaseConfiguration.create()
    //conf.set("hbase.zookeeper.property.clientPort", "2181")
    //conf.set("hbase.zookeeper.quorum", "sparkmaster1")
    conf.set(TableOutputFormat.OUTPUT_TABLE, "hbase_test")

    val job = Job.getInstance(conf)

    job.setOutputKeyClass(classOf[ImmutableBytesWritable])

    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val rawData = Array((1, "Jazz", 14), (2, "Andy", 18), (3, "Vincent", 38))

    val xdata = spark.sparkContext.parallelize(rawData).map{
      case (key,name,age) =>{
        val p = new Put(Bytes.toBytes(key))
        p.setDurability(Durability.SKIP_WAL)
        p.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes(name))
        p.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(age))
        //p.addImmutable("cf1","name",name)
        (new ImmutableBytesWritable, p)
      }
    }
    xdata.saveAsNewAPIHadoopDataset(job.getConfiguration)

    spark.stop()

  }
}
