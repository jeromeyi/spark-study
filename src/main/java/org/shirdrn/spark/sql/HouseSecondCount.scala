package org.shirdrn.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object HouseSecondCount {
  
  case class houseSecond( client:String,city:String, keyword:String,area:String)  
  def main(args: Array[String]) {  
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")  
    //val sparkConf = new SparkConf().setMaster("spark://master:7077").setAppName("SparkSql")  
    val sc  = new SparkContext(sparkConf)  
    //val ssc = new StreamingContext(sc, Seconds(10))  
    val sqlContext = new SQLContext(sc)  
     
    //val houseseconddata = sc.textFile("/maitian/HouseSecond/HouseSecond*")
    /*val houseseconddata = sc.sequenceFile[UserID, UserInfo]("hdfs://master:9000/maitian/HouseSecond/HouseSecond*")
      .partitionBy(new HashPartitioner(100)) // 构造100个分区
      .persist()*/
     val houseseconddata = sc.textFile("hdfs://master:9000/maitian/HouseSecond/HouseSecond*")
     val housesecond = houseseconddata.map (
      line =>  line.split(",")).filter(_.length>20).map(data=>
        houseSecond(data(0),data(1),data(2).replaceAll("\"", ""),data(15)) )

    /*val housesecond=houseseconddata.map(line=>line.split(",")).map{
      s =>s.length>20
        houseSecond(s(0),s(1),s(2).replaceAll("\"", ""),s(15))  
    }*/
    import sqlContext.implicits._  
    val  dfhousesecond= housesecond.toDF()
    dfhousesecond.select("aa").show();
    dfhousesecond.registerTempTable("housesecond")  
    dfhousesecond.show()
  }
}