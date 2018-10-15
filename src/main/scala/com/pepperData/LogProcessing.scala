package com.pepperData

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import scala.util.Try
import sys.process._
import scala.util.Try
import org.apache.hadoop.fs._
import org.apache.spark.sql.functions._
import java.util.Properties
import org.apache.spark.sql.SparkSession
import com.pepperData.LogUtilities.AccessLogParser
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.Encoders
import scala.concurrent.duration._
import org.apache.kafka.clients.consumer._



object LogProcessing {

	def main(args:Array[String]){
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)
		Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.OFF)
		val spark = SparkSession.builder().appName("PepperJobTest").master("local[*]").getOrCreate()
		spark.conf.set("spark.ui.showConsoleProgress", false)
		val propFile = args(0)
		import spark.implicits._
		var data = new Properties();
		data.load(new FileInputStream(propFile))
		val jobMode = data.getProperty("mode")
		if (jobMode.equalsIgnoreCase("batch")){
		  batchProcessing(data,spark)
		}
		else if (jobMode.equalsIgnoreCase("kafkaStreaming")){
		  streamProcessing(data,spark)
		}	
	}
	def batchProcessing(data:Properties,spark: SparkSession):Unit={
			import spark.implicits._
			val logFileLocation=data.getProperty("logfile")
			val logs = spark.read.textFile(logFileLocation)
			val parser = new AccessLogParser
			val logsData = logs.map(x=>{
				parser.parseRecordReturningNullObjectOnFailure(x)
			})
			println("The schema of the logs dataset is as follows")
			logsData.printSchema()
			logsData.createOrReplaceTempView("logstable")
			println("The top 10 IP addresses based on frequency of access")
			spark.sql("select clientIpAddress as IP,count(*) as frequency from logstable group by clientIpAddress order by frequency desc").show(10)
			logsData.createOrReplaceTempView("logstable")
			println("Top HTTP status messages with their frequency")
			spark.sql("select requestType ,count(*) as frequency from logstable group by requestType order by frequency desc").show(10)
			spark.close()
	}
	
	def streamProcessing(data:Properties,spark: SparkSession):Unit={
			import spark.implicits._
			val kafkaBrokers = data.getProperty("brokers")
			val topic = data.getProperty("topic")
			val parser = new AccessLogParser
			val logs = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers)
			.option("subscribe", topic)
			.option("startingOffsets", "latest")
			.load().createOrReplaceTempView("logstreamingview")
		  spark.sql("select cast (value as string) as value from logstreamingview").map(x=>{
				parser.parseRecordReturningNullObjectOnFailure(x.getString(0))
			}).coalesce(10).createOrReplaceTempView("logstableView")
			spark.sql("select clientIpAddress,count(*) as frequency from logstableView group by clientIpAddress order by frequency desc").writeStream.
			outputMode("complete").
			format("console").
			option("truncate", false).
			option("numRows", 10).
			trigger(Trigger.ProcessingTime(1.minute)).
			queryName("groupByIPaddress").
			start.awaitTermination()
			
	}

}