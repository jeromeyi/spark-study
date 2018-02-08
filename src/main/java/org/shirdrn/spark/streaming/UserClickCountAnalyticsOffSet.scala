package org.shirdrn.spark.streaming

import kafka.message.MessageAndMetadata
import kafka.utils.ZKGroupTopicDirs
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.InputDStream
import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import net.sf.json.JSONObject
import org.apache.spark.streaming.kafka.KafkaCluster
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool




object UserClickCountAnalyticsOffSet {
   def main(args: Array[String]): Unit = {
    var masterUrl = "local[1]"
    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Kafka configurations
    //val topics = Set("user_events")
    //val brokers = "master:9092,slave1:9092,slave2:9092"
    val brokers = "192.168.81.220:9092,192.168.81.221:9092,192.168.81.222:9092"
    val kafkaParam = Map[String, String](
      "metadata.broker.list" -> brokers, 
       // "bootstrap.servers" -> brokers,
      "zookeeper.connect" ->"master:2181,slave1:2181,slave2:2181",
      //"zookeeper.session.timeout.ms" ->"400",
      //"zookeeper.sync.time.ms" ->"200",
      "group.id" -> "click",
      "client.id" ->"test1",
      "consumer.id" -> "c1",
      "auto.offset.reset" -> "smallest"
      //"auto.commit.enable" -> "false",
      //"auto.commit.interval.ms" -> "100"
      //"serializer.class" -> "kafka.serializer.StringEncoder"
      )
     val topic : String = "user_events"  //消费的 topic 名字
    val topics : Set[String] = Set(topic)                    //创建 stream 时使用的 topic 名字集合
  val clickHashKey = "app::users::click"
    val topicDirs = new ZKGroupTopicDirs("test_spark_streaming_group", topic)  //创建一个 ZKGroupTopicDirs 对象，对保存
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"          //获取 zookeeper 中的路径，这里会变成 /consumers/test_spark_streaming_group/offsets/topic_name
  
    val zkClient = new ZkClient("master:2181,slave1:2181,slave2:2181")          //zookeeper 的host 和 ip，创建一个 client
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
  
    var kafkaStream : InputDStream[(String, String)] = null 
    var fromOffsets: Map[TopicAndPartition, Long] = Map()  //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
  
    if (children > 0) {  //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
        for (i <- 0 until children) {
          val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
          val tp = TopicAndPartition(topic, i)
          fromOffsets += (tp -> partitionOffset.toLong)  //将不同 partition 对应的 offset 增加到 fromOffsets 中
          //logInfo("@@@@@@ topic[" + topic + "] partition[" + i + "] offset[" + partitionOffset + "] @@@@@@")
        }
  
        val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())  //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
        kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParam, fromOffsets, messageHandler)
    }
    else {
        kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topics) //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offset
    }
    var offsetRanges = Array[OffsetRange]()
   /* val userClicks =kafkaStream.transform{ rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
      rdd
    }*/
    kafkaStream.foreachRDD { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)  //将该 partition 的 offset 保存到 zookeeper
        //logInfo(s"@@@@@@ topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset}  untiloffset ${o.untilOffset} #######")
      }
  
    }
    
    
    val userClicks =kafkaStream.flatMap(line => {
      val data = JSONObject.fromObject(line._2)
      Some(data)
    }).map(x => (x.getString("uid"), x.getInt("click_count"))).reduceByKey(_ + _)
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
         
          /**
           * Internal Redis client for managing Redis connection {@link Jedis} based on {@link RedisPool}
           */
          object InternalRedisClient extends Serializable {
           
            @transient private var pool: JedisPool = null
           
            def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
                maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
              makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)   
            }
           
            def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
                maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean,
                testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
              if(pool == null) {
                   val poolConfig = new GenericObjectPoolConfig()
                   poolConfig.setMaxTotal(maxTotal)
                   poolConfig.setMaxIdle(maxIdle)
                   poolConfig.setMinIdle(minIdle)
                   poolConfig.setTestOnBorrow(testOnBorrow)
                   poolConfig.setTestOnReturn(testOnReturn)
                   poolConfig.setMaxWaitMillis(maxWaitMillis)
                   pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)
                  
                   val hook = new Thread{
                        override def run = pool.destroy()
                   }
                   sys.addShutdownHook(hook.run)
              }
            }
           
            def getPool: JedisPool = {
              assert(pool != null)
              pool
            }
          }
         
          // Redis configurations
          val maxTotal = 10
          val maxIdle = 10
          val minIdle = 1
          val redisHost = "slave2"
          val redisPort = 7008
          val redisTimeout = 30000
          val dbIndex = 1
          InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)
         
          val uid = pair._1
          val clickCount = pair._2
          val jedis =InternalRedisClient.getPool.getResource
          //println(jedis.)
          //jedis.select(dbIndex)
          //println(uid+"---"+clickCount)
          jedis.hincrBy(clickHashKey, uid, clickCount)
          InternalRedisClient.getPool.returnResource(jedis)
        })
      })
    })
    userClicks.print()
    ssc.start()
    ssc.awaitTermination()
   }
   
}