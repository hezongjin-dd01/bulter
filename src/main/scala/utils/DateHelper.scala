package utils

import java.text.SimpleDateFormat
import java.util.Date

object DateHelper {

  def getYesterday = {

    val timestamp = System.currentTimeMillis() - 24 * 60 * 60 * 1000
    val date = new Date(timestamp)
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val str = format.format(date)
    str
  }

  def getNowTimestamp(): String = {
    val timestamp = System.currentTimeMillis()
    val date = new Date(timestamp)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val str = format.format(date)
    str
  }

  def main(args: Array[String]): Unit = {
    println(getNowTimestamp())
  }
}
