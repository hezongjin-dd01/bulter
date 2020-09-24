package etl

import java.util.Properties

import beans.MGM_OPER_LOG
import com.alibaba.fastjson.JSON
import comm.Constants
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import utils.{DateHelper, KafkaHelper, SparkHelper}

import scala.collection.mutable.ArrayBuffer

object etl_mgm_oper_log2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkHelper.getSparkSession(Constants.NAME, "yarn")
    spark.sparkContext.setLogLevel("WARN")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(Constants.SECONDS))

    //    val df = getDataframe(spark, Constants.SITE_URL, Constants.SITE_TABLE, Constants.SITE_USER, Constants.SITE_PASSWORD)

    //    val siteList = df.rdd.map(row => row.getAs("ID").toString).collect().toList
    val productList = spark.sparkContext.makeRDD(Constants.PRODUCT_LIST).collect().toList
    val contentList = spark.sparkContext.makeRDD(Constants.CONTENT_LIST).collect().toList


    //    val siteListValue = spark.sparkContext.broadcast(siteList)
    val productListBroadcast = spark.sparkContext.broadcast(productList)
    val contentListBroadcast = spark.sparkContext.broadcast(contentList)


    val kafkaParams = KafkaHelper.getKafkaParams(Constants.KAFKA_SERVERS, Constants.GROUP_ID, Constants.LATEST)
    val topics = Array(Constants.TOPIC)

    val offsetMap = KafkaHelper.getOffset(Constants.GROUP_ID, Constants.TOPIC)

    val stream = if (offsetMap.size == 0) {
      println("===========第一次进行消费============")
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )
    } else {
      println("============已经有偏移量了===============")
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsetMap)
      )
    }

    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {

        val nowTime = DateHelper.getNowTimestamp()
        println(s"${nowTime}===============\t" + rdd.count)

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val mapRdd: RDD[MGM_OPER_LOG] = rdd.mapPartitions(iter => {
          val arr = new ArrayBuffer[MGM_OPER_LOG]()
          //          val siteListWorker = siteListValue.value
          val productListValue = productListBroadcast.value
          val contentListValue = contentListBroadcast.value

          while (iter.hasNext) {
            val value = iter.next().value()

            val jsonObject = JSON.parseObject(value)
            val table = jsonObject.getString("table")

            if ("MGM_OPER_LOG".equals(table)) {
              //              println(value)

              val siteId = jsonObject.getString("database").substring(5)
              val data = jsonObject.getString("data")

              if (data != null) {

                val jsonArray = JSON.parseArray(data)

                for (i <- 0 to jsonArray.size() - 1) {
                  var index = false

                  val json = jsonArray.getJSONObject(i)
                  val log_id = json.getString("LOG_ID")
                  val login_name = json.getString("LOGIN_NAME")
                  val login_ip = json.getString("LOGIN_IP")
                  val user_browser_name = json.getString("USER_BROWSER_NAME")
                  val user_browser_version = json.getString("USER_BROWSER_VERSION")
                  val user_os_name = json.getString("USER_OS_NAME")
                  val user_login_device = json.getString("USER_LOGIN_DEVICE")
                  val create_date_time = json.getString("CREATE_DATE_TIME")
                  val oper_content = json.getString("OPER_CONTENT")
                  val request_parameters = json.getString("REQUEST_PARAMETERS")
                  val tid = json.getString("TID")
                  val day = create_date_time.substring(0, 10)
                  var tag = ""


                  productListValue.foreach(url => {
                    if (oper_content.contains(url)) {
                      tag = "content"
                      index = true
                    }
                  })

                  contentListValue.foreach(url => {
                    if (oper_content.contains(url)) {
                      tag = "content"
                      index = true
                    }
                  })


                  if (index == true) {
                    arr += MGM_OPER_LOG(siteId, log_id, login_name, login_ip, user_browser_name, user_browser_version,
                      user_os_name, user_login_device, create_date_time, oper_content, request_parameters, tid, tag, day)
                  }

                }

              }

            }

          }

          arr.iterator
        })

        import spark.implicits._

        val df2 = mapRdd.toDF()
        df2.createOrReplaceTempView("tmp")

        spark.sql(Constants.sql2)


        KafkaHelper.saveOffset(Constants.GROUP_ID, offsetRanges)
      }
    })


    //
    ssc.start()
    ssc.awaitTermination()

  }
}
