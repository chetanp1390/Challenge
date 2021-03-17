package com.demo

import java.util.Properties
import java.io.File
import java.nio.file.{Files, Path, StandardCopyOption}
import org.apache.kafka.clients.producer._
import scala.io.Source
import java.io.File

object ProducerDemo {
  def main(args: Array[String]): Unit = {

    val topics = "DDosTest"
    
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val filedir = new File("C:/Users/insan/Downloads/phdata/logs")
    val filelist = filedir.listFiles.filter(_.isFile).map(_.getPath).toList
    
    filelist.foreach(f =>
      Source.fromFile(f).getLines.foreach(line => {
      val record = new ProducerRecord[String, String](topics, "key", line)
      producer.send(record)
      }) 
      )
    //producer.close()
  }
}