package com.example.spark
// test commit from eclipse
// test2
import kafka.serializer.StringDecoder
import org.apache.spark.{TaskContext, SparkConf}
import org.apache.spark.streaming.kafka.{OffsetRange, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.dstream._
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import java.util.HashMap
import java.nio.ByteBuffer

object DirectKafkaWordCount {
  val kafkaBrokers = "kafka:9092"
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    // Create context with 10 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCountStream = words.map(x => (x, 1L)).reduceByKey(_ + _)
    // wordCounts.print()

    wordCountStream.foreachRDD( rdd => {
    System.out.println("# events = " + rdd.count())
    
    rdd.foreachPartition( partition => {
      // Print statements in this section are shown in the executor's stdout logs
    val kafkaOpTopic = "output" 
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
             "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer")
    
    val producer = new KafkaProducer[String, String](props)
    partition.foreach( record => {
      val data = record.toString
      // As as debugging technique, users can write to DBFS to verify that records are being written out 
    // dbutils.fs.put("/tmp/test_kafka_output",data,true)
          val message = new ProducerRecord[String, String](kafkaOpTopic, null, data)      
          producer.send(message)
        } )
        producer.close()
       })
    
    })
      
    
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
