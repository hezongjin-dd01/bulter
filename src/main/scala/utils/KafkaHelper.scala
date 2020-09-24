package utils

import java.sql.DriverManager

import comm.Constants
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable

object KafkaHelper {

  def getKafkaParams(servers: String, groupid: String, reset: String) = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> servers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> reset, // latest||earliest
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    kafkaParams
  }

  def getOffset(groupid: String, topic: String) = {
    Class.forName("com.mysql.jdbc.Driver");
    val connection = DriverManager.getConnection(Constants.MYSQL_URL, Constants.USER, Constants.PASSWORD)
    val pstmt = connection.prepareStatement("select * from t_offset where groupid=? and topic =?")

    pstmt.setString(1, groupid)
    pstmt.setString(2, topic)

    val rs = pstmt.executeQuery()

    val offsetMap = mutable.Map[TopicPartition, Long]()

    while (rs.next()) {
      offsetMap += (new TopicPartition(rs.getString("topic"), rs.getInt("partition")) -> rs.getLong("offset"))
    }
    rs.close()
    pstmt.close()
    connection.close()

    offsetMap
  }

  def saveOffset(groupid: String, offsetRange: Array[OffsetRange]): Unit = {
    Class.forName("com.mysql.jdbc.Driver");
    val connection = DriverManager.getConnection(Constants.MYSQL_URL, Constants.USER, Constants.PASSWORD)

    val pstmt = connection.prepareStatement("replace into t_offset (`topic`,`partition`,`groupid`,`offset`) values (?,?,?,?)")

    for (o <- offsetRange) {
      pstmt.setString(1, o.topic)
      pstmt.setInt(2, o.partition)
      pstmt.setString(3, groupid)
      pstmt.setLong(4, o.untilOffset)
      pstmt.executeUpdate()
    }

    pstmt.close()
    connection.close()
  }
}
