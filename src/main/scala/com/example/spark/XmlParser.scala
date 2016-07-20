package com.example.spark


import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.{OffsetRange, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.{DataFrame,SQLContext}
import com.databricks.spark.xml
import kafka.serializer.StringDecoder
import java.util.HashMap
import java.nio.ByteBuffer

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
// import org.json4s.Xml.{toJson, toXml}
import org.json._

object XmlParser {
  val kafkaBrokers = "kafka:9092"
  def  main(args : Array[String]) = {
    
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

    
    val sparkConf = new SparkConf().setAppName("XmlParser").setMaster("local")
    val sc = new SparkContext(sparkConf)
    
    val ssc = new StreamingContext(sc, Seconds(10))
    val sqlContext : SQLContext = new org.apache.spark.sql.SQLContext(sc)
    
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    
    val xmlMessage = messages.map(_._2)
    
    val jsonStream = messages.transform(
      y => {
        y.filter(x => x._1 != null && x._2 != null).map(x => {
         
          XML.toJSONObject(x._2).toString(4);
        }
        )
      })
    
     /////////////////////////////////////////
      
    jsonStream.foreachRDD( rdd => {
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
      import sqlContext.implicits._     
      sqlContext.read.json(record).registerTempTable("channel")
       
      val weatherRDD = sqlContext.sql(""" select item.forecast from channel """).rdd
      val data = weatherRDD.toString
      // As as debugging technique, users can write to DBFS to verify that records are being written out 
    // dbutils.fs.put("/tmp/test_kafka_output",data,true)
          val message = new ProducerRecord[String, String](kafkaOpTopic, null, data)      
          producer.send(message)
        } )
        producer.close()
       })
    
    }) 
      
      
      
      ///////////////////////////
      
      
     // val weatherRDD = parseWeather(sqlContext)
    
    
    
  }
  
  /*def parseWeather(sqlContext : SQLContext) = {
    
    var df : DataFrame = null
    var newDf : DataFrame = null
    import sqlContext.implicits._
    
    df = sqlContext.read.format("xml").option("rowTag", "channel").load("data/weather.xml")
    
    //  df.printSchema()
    df.registerTempTable("channel")
    

    val weatherRDD = sqlContext.sql(""" select item.forecast from channel """).rdd
    weatherRDD
  }*/
  
}