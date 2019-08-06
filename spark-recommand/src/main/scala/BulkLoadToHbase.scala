import java.util.{ArrayList, UUID}

import CommonFuction.{filterLog, formatUserLog}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

/**
  * 用户日志保存到hbase app
  */
object BulkLoadToHbase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .config("hive.metastore.uris","thrift://master01:9083")//SPARK读取hive中的元数据必须配置metastore地址
      .config("spark.sql.warehouse.dir", "hdfs://nameservice1/user/hive/warehouse")//配置hive的数据存储目录
      .appName(SaveToHbase.getClass.getSimpleName)
      .enableHiveSupport()
      .getOrCreate


    import spark.implicits._
    //val newsLogFrame = spark.read.textFile(args(0)).rdd.map(formatUserLog).filter(filterLog).toDF()
    val newsLogFrame = spark.read.parquet(args(0)).filter("un != ''")

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "master02")
    //必须提前sorted排序，由于hbase存储数据是有序的，而读取hfile的时候如果不提前排序会报错
    var arrName = Array("ac","ci","dt","gki","rcc","rkd","ri","sw","ua","un","vt").sorted
    val pathString = "/tmp/user_log"
    var path = new Path(pathString)
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if(hdfs.exists(path)){
      //为防止误删，禁止递归删除
      hdfs.delete(path,true)
    }
    println("-------------------导入日志位置为："+args(0)+"Hfile临时目录存储位置为："+pathString)
    val HFile = newsLogFrame.rdd.map(row => {
      val visitTime = row.getAs[String]("vt")

      val actionType = row.getAs[String]("ac")

      val clientIP = row.getAs[String]("ci")

      //val rowKey = visitTime.toLong.hashCode() % 10 + visitTime
      val rowKey = UUID.randomUUID()+ "-" + visitTime

      val family = "cf1"
      (rowKey,row)

    })
      .sortByKey()
        .flatMap{
          case (rowKey,row)=>{
            for(i <- 0 to arrName.length - 1) yield {
              //(new ImmutableBytesWritable(Bytes.toBytes(rowKey))
              var kv =new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes("cf1"), Bytes.toBytes(arrName(i)), Bytes.toBytes(row.getAs[String](arrName(i))))
              (new ImmutableBytesWritable(Bytes.toBytes(rowKey)),kv)
            }
          }
        }

    HFile.saveAsNewAPIHadoopFile(pathString, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], conf)

    //开始即那个HFile导入到Hbase,此处都是hbase的api操作
    val load = new LoadIncrementalHFiles(conf)
    //hbase的表名
    val tableName = "user_log"
    //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
    val conn = ConnectionFactory.createConnection(conf)
    //根据表名获取表
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    try {
      //创建一个hadoop的mapreduce的job
      val job = Job.getInstance(conf)
      //设置job名称
      job.setJobName("DumpFile")
      //此处最重要,需要设置文件输出的key,因为我们要生成HFil,所以outkey要用ImmutableBytesWritable
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      //输出文件的内容KeyValue
      job.setMapOutputValueClass(classOf[KeyValue])
      //配置HFileOutputFormat2的信息
      HFileOutputFormat2.configureIncrementalLoadMap(job, table)
      //开始导入
      val start=System.currentTimeMillis()
      load.doBulkLoad(path, table.asInstanceOf[HTable])
      val end=System.currentTimeMillis()
      println("用时："+(end-start)+"毫秒！")
    } finally {
      table.close()
      conn.close()
    }
    spark.stop()
  }
}
