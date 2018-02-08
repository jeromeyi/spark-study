package org.shirdrn.spark.sql.ibm

import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{ SparkContext, SparkConf }

//define case class for user
case class User(userID: String, gender: String, age: Int,
                registerDate: String, role: String, region: String)
//define case class for consuming data
case class Order(orderID: String, orderDate: String, productID: Int, price: Int, userID: String)
/**
 * 
 ./spark-submit –class com.ibm.spark.exercise.sql.UserConsumingDataStatistics \
 --master spark://<spark_master_node_ip>:7077 \
--num-executors 6 \
--driver-memory 8g \
--executor-memory 2g \
--executor-cores 2 \
/home/fams/spark_exercise-1.0.jar \ 
 hdfs://<hdfs_namenode_ip>:9000/user/fams/inputfiles/sample_user_data.txt \ 
 hdfs://<hdfs_namenode_ip>:9000/user/fams/inputfiles/sample_consuming_data.txt
 */
object UserConsumingDataStatistics {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage:UserConsumingDataStatistics userDataFilePath consumingDataFilePath")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Spark Exercise:User Consuming Data Statistics")
    //Kryo serializer is more quickly by default java serializer
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ctx = new SparkContext(conf)
    val sqlCtx = new SQLContext(ctx)
    import sqlCtx.implicits._
    //Convert user data RDD to a DataFrame and register it as a temp table
    val userDF = ctx.textFile(args(0)).map(_.split(" ")).map(
      u => User(u(0), u(1), u(2).toInt, u(3), u(4), u(5))).toDF()
    userDF.registerTempTable("user")
    //Convert consuming data RDD to a DataFrame and register it as a temp table
    val orderDF = ctx.textFile(args(1)).map(_.split(" ")).map(o => Order(
      o(0), o(1), o(2).toInt, o(3).toInt, o(4))).toDF()
    orderDF.registerTempTable("orders")
    //cache the DF in memory with serializer should make the program run much faster
    userDF.persist(StorageLevel.MEMORY_ONLY_SER)
    orderDF.persist(StorageLevel.MEMORY_ONLY_SER)

    //The number of people who have orders in the year 2015
    val count = orderDF.filter(orderDF("orderDate").contains("2015")).join(
      userDF, orderDF("userID").equalTo(userDF("userID"))).count()
    println("The number of people who have orders in the year 2015:" + count)
    //total orders produced in the year 2014
    val countOfOrders2014 = sqlCtx.sql("SELECT * FROM orders where orderDate like '2014%'").count()
    println("total orders produced in the year 2014:" + countOfOrders2014)
    //Orders that are produced by user with ID 1 information overview
    val countOfOrdersForUser1 = sqlCtx.sql("SELECT o.orderID,o.productID,o.price,u.userID FROM orders o,user u where u.userID =1 and u.userID = o.userID").show()
    println("Orders produced by user with ID 1 showed.")
    //Calculate the max,min,avg prices for the orders that are producted by user with ID 10
    val orderStatsForUser10 = sqlCtx.sql("SELECT max(o.price) as maxPrice,min(o.price) as minPrice,avg(o.price) as avgPrice,u.userID FROM orders o,user u where u.userID = 10 and u.userID = o.userID group by u.userID")
    println("Order statistic result for user with ID 10:")
    orderStatsForUser10.collect().map(order => "Minimum Price=" + order.getAs("minPrice")
      + ";Maximum Price=" + order.getAs("maxPrice")
      + ";Average Price=" + order.getAs("avgPrice")).foreach(result => println(result))
  }
}