package org.shirdrn.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zpf on 2016/8/23.
  */
/**
  * 使用Scala并发集群运行的Spark来实现在线热搜词
  *
  * 背景描述：在社交网络（例如微博），电子商务（例如京东），热搜词（例如百度）等人们核心关注的内容之一就是我所关注的内容中
  * 大家正在最关注什么或者说当前的热点是什么，这在市级企业级应用中是非常有价值，例如我们关心过去30分钟大家正在热搜什么，并且
  * 每5分钟更新一次，这就使得热点内容是动态更的，当然更有价值。
  * Yahoo（是Hadoop的最大用户）被收购，因为没做到实时在线处理
  * 实现技术：Spark Streaming（在线批处理） 提供了滑动窗口的奇数来支撑实现上述业务背景，我外面您可以使用reduceByKeyAndWindow操作来做具体实现
  *
  */
object OnlineHottestItems {
  def main(args: Array[String]){
    /**
      * 第1步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，
      * 例如说通过setMaster来设置程序要链接的Spark集群的Master的URL,如果设置
      * 为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差（例如
      * 只有1G的内存）的初学者       *
      */
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("OnlineHottestItems") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    conf.setMaster("spark://Master:7077") //此时，程序在Spark集群

/*
* 此处设置 Batch Interval 实在spark Streaming 中生成基本Job的单位，窗口和滑动时间间隔
* 一定是该batch Interval的整数倍*/
    val ssc = new StreamingContext(conf, Seconds(5))

    val hottestStream = ssc.socketTextStream("Master", 9999)

    /*
    * 用户搜索的格式简化为 name item,在这里我们由于要计算热点内容，所以只需要提取item即可
    * 提取出的item通过map转化为（item,1）形式
    * 每隔20秒更新过去60秒的内容窗口60秒，滑动20秒
    * */

    val searchPair = hottestStream.map(_.split(" ")(1)).map(item => (item , 1))
    val hottestDStream = searchPair.reduceByKeyAndWindow((v1:Int,v2:Int) => v1 + v2, Seconds(60) ,Seconds(20))

    hottestDStream.transform(hottestItemRDD => {
    val top3 =  hottestItemRDD.map(pair => (pair._2,pair._1) ).sortByKey(false).
      map(pair => (pair._2,pair._1)).take(3)

      for(item <- top3){
        println(item)
      }
      hottestItemRDD
    }).print()

    ssc.start()
    ssc.awaitTermination()

  }
}