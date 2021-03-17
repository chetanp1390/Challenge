package com.demo

import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SaveMode


object Ddos {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:/hadoop")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group_demo",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val conf = new SparkConf().setAppName("ddosdemo").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(60))
 
    ssc.sparkContext.setLogLevel("ERROR")
     
    val sqlContext = new org.apache.spark.sql.SQLContext(ssc.sparkContext)
    
    import sqlContext.implicits._
    
    val threshold = 88
    val topics = Array("DDosTest")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    val IPpairs = stream.map(record => (record.value.split(" ")(0), 1))

    val reduceByKeyFunc = (count: Int, anotherCount: Int) => count + anotherCount
    
    val IPsCounts = IPpairs.reduceByKey(reduceByKeyFunc)
    
    val counts = IPsCounts.filter{ case (ip, count) => count > threshold }

    //counts.print()
    val check = counts.map{ case (ip, count) => ip }
    
    check.foreachRDD(rdd => {
    if (!rdd.isEmpty()) {
      //rdd.toDF("IP").show
      rdd.toDF("IP").coalesce(1).write.format("csv").mode(SaveMode.Append).save("C:/data/spark/")
    }

})
    ssc.start()
    ssc.awaitTermination()
  }
}