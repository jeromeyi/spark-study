package org.shirdrn.spark.sql.ibm

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

/**
 ./spark-submit --class com.ibm.spark.exercise.sql.PeopleDataStatistics2 \
--master spark://<spark_master_node_ip>:7077 \
--driver-memory 8g \
--executor-memory 2g \
--total-executor-cores 12 \
 /home/fams/spark_exercise-1.0.jar \ hdfs://<hdfs_namenode_ip>:9000/
                                  user/fams/inputfiles/sample_people_info2.txt
 */
object PeopleDataStatistics2 {
  private val schemaString = "id,gender,height"
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage:PeopleDataStatistics2 filePath")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Spark Exercise:People Data Statistics 2")
    val sc = new SparkContext(conf)
    val peopleDataRDD = sc.textFile(args(0))
    val sqlCtx = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlCtx.implicits._
    val schemaArray = schemaString.split(",")
    val schema = StructType(schemaArray.map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD: RDD[Row] = peopleDataRDD.map(_.split(" ")).map(
      eachRow => Row(eachRow(0), eachRow(1), eachRow(2)))
    val peopleDF = sqlCtx.createDataFrame(rowRDD, schema)
    peopleDF.registerTempTable("people")
    //get the male people whose height is more than 180
    val higherMale180 = sqlCtx.sql("select id,gender,height from people where height > 180 and gender='M'")
    println("Men whose height are more than 180: " + higherMale180.count())
    println("<Display #1>")
    //get the female people whose height is more than 170
    val higherFemale170 = sqlCtx.sql("select id,gender,height from people where height > 170 and gender='F'")
    println("Women whose height are more than 170: " + higherFemale170.count())
    println("<Display #2>")
    //Grouped the people by gender and count the number
    peopleDF.groupBy(peopleDF("gender")).count().show()
    println("People Count Grouped By Gender")
    println("<Display #3>")
    //
    peopleDF.filter(peopleDF("gender").equalTo("M")).filter(
      peopleDF("height") > 210).show(50)
    println("Men whose height is more than 210")
    println("<Display #4>")
    //
    peopleDF.sort($"height".desc).take(50).foreach { row => println(row(0) + "," + row(1) + "," + row(2)) }
    println("Sorted the people by height in descend order,Show top 50 people")
    println("<Display #5>")
    //
    peopleDF.filter(peopleDF("gender").equalTo("M")).agg(Map("height" -> "avg")).show()
    println("The Average height for Men")
    println("<Display #6>")
    //
    peopleDF.filter(peopleDF("gender").equalTo("F")).agg("height" -> "max").show()
    println("The Max height for Women:")
    println("<Display #7>")
    //......
    println("All the statistics actions are finished on structured People data.")
  }
}