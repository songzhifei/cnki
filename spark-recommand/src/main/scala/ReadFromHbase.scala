import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.sql.SparkSession

/**
  * spark读取hbase数据 demo app
  */
object ReadFromHbase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(SaveToHbase.getClass.getSimpleName)
      .getOrCreate

    val tablename = "user_log"
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum","master02")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tablename)

    val scan = new Scan(Bytes.toBytes("00000993-378a-4025-90a2-d437ea913adb"),Bytes.toBytes("00000993-378a-4025-90a2-d437ea913adb"))

    //将scan类转化成string类型
    val proto= ProtobufUtil.toScan(scan)

    val ScanToString = Base64.encodeBytes(proto.toByteArray)
    conf.set(TableInputFormat.SCAN,ScanToString)

    //读取数据并转化成rdd
    val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val count = hBaseRDD.count()
    println(count)
    hBaseRDD.foreach{case (_,result) =>{
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val citycode = Bytes.toString(result.getValue("cf1".getBytes,"ac".getBytes))
      val daytime = Bytes.toInt(result.getValue("cf1".getBytes,"ci".getBytes))
      println("Row key:"+key+" ac:"+citycode+" ci:"+daytime)
    }}

    spark.stop()
  }
}
