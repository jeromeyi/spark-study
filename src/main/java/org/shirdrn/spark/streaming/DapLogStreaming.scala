package org.shirdrn.spark.streaming

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.KafkaUtils
/**
 * auth:lxw1234
 * http://lxw1234.com
 */
/**
 *
 * 实时统计需求
 *
 * 以60秒为间隔，统计60秒内的pv,ip数,uv
 *
 * 最终结果包括：
 *
 * 时间点：pv：ips：uv
 *
 * 原始日志格式
 *
 * 2015-11-11T14:59:59|~|xxx|~|202.109.201.181|~|xxx|~|xxx|~|xxx|~|B5C96DCA0003DB546E7
 * 2015-11-11T14:59:59|~|xxx|~|125.119.144.252|~|xxx|~|xxx|~|xxx|~|B1611D0E00003857808
 * 2015-11-11T14:59:59|~|xxx|~|125.119.144.252|~|xxx|~|xxx|~|xxx|~|1555BD0100016F2E76F
 * 2015-11-11T15:00:00|~|xxx|~|125.119.144.252|~|xxx|~|xxx|~|xxx|~|C0EA13670E0B942E70E
 * 2015-11-11T15:00:00|~|xxx|~|125.119.144.252|~|xxx|~|xxx|~|xxx|~|C0EA13670E0B942E70E
 * 2015-11-11T15:00:01|~|xxx|~|125.119.144.252|~|xxx|~|xxx|~|xxx|~|4E3512790001039FDB9
 * 每条日志包含7个字段，分隔符为|~|，其中，第3列为ip，第7列为cookieid。假设原始日志已经由Flume流到Kafka中。
 *
 * 运行SparkStreaming程序
 *
 * 进入$SPARK_HOME/bin执行下面的命令，将SparkStreaming程序提交到YARN：
 *
 * ./spark-submit \
 * --class com.lxw.test.DapLogStreaming \
 * --master yarn-cluster \
 * --executor-memory 2G \
 * --num-executors 6 \
 * --jars /home/liuxiaowen/kafka-clients-0.8.2.1.jar,/home/liuxiaowen/metrics-core-2.2.0.jar,/home/liuxiaowen/zkclient-0.3.jar,/home/liuxiaowen/spark-streaming-kafka_2.10-1.5.0.jar,/home/liuxiaowen/kafka_2.10-0.8.2.1.jar \
 * /home/liuxiaowen/DapLogStreaming.jar
 */
object DapLogStreaming {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("yarn-cluster").setAppName("DapLogStreaming")
    //每60秒一个批次
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    //从Kafka中读取数据，topic为daplog，该topic包含两个分区
    val kafkaStream = KafkaUtils.createStream(
      ssc,
      "bj11-65:2181", //Kafka集群使用的zookeeper
      "group_spark_streaming", //该消费者使用的group.id
      Map[String, Int]("daplog" -> 0, "daplog" -> 1), //日志在Kafka中的topic及其分区
      StorageLevel.MEMORY_AND_DISK_SER)
      .map(x => x._2.split("\\|~\\|", -1)) //日志以|~|为分隔符

    kafkaStream.foreachRDD((rdd: RDD[Array[String]], time: Time) => {
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      //构造case class: DapLog,提取日志中相应的字段
      val logDataFrame = rdd.map(w => DapLog(w(0).substring(0, 10), w(2), w(6))).toDF()
      //注册为tempTable
      logDataFrame.registerTempTable("daplog")
      //查询该批次的pv,ip数,uv
      val logCountsDataFrame =
        sqlContext.sql("select date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as time,count(1) as pv,count(distinct ip) as ips,count(distinct cookieid) as uv from daplog")
      //打印查询结果
      logCountsDataFrame.show()
    })

    ssc.start()
    ssc.awaitTermination()

  }

}

case class DapLog(day: String, ip: String, cookieid: String)

object SQLContextSingleton {
  @transient private var instance: SQLContext = _
  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}