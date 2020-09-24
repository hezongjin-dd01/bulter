package utils

import beans.MGM_OPER_LOG
import comm.Constants
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkHelper {

  def getStreamingContext(name: String, master: String) = {

    val conf = new SparkConf().setAppName(name).setMaster(master)
      .set("hive.exec.dynamici.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "1000")
      .set("spark.streaming.kafka.initialRate", "500")
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.rdd.compress", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    //      .set("hive.exec.stagingdir", "/tmp/hive/staging/.hive-staging")

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.sparkContext.setLogLevel(Constants.LOG_LEVEL)

    val ssc = new StreamingContext(spark.sparkContext, Seconds(Constants.SECONDS))

    ssc
  }

  def getSparkSession(name: String, master: String) = {
    val conf = new SparkConf().setAppName(name).setMaster(master)
      .set("hive.exec.dynamici.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "1000")
      .set("spark.streaming.kafka.initialRate", "500")
      .set("spark.rdd.compress", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("hive.exec.stagingdir", "tmp/hive/.hive-staging")
      .set("spark.sql.shuffle.partitions", "20")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "1024k")
      .set("spark.kryoserializer.buffer.max", "1024m")
    //      .set("spark.sql.hive.convertMetastoreParquet", "false")

    conf.registerKryoClasses(Array(
      classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
      classOf[MGM_OPER_LOG]
    ))

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark
  }


}
