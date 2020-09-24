package utils

object test {

  def main(args: Array[String]): Unit = {

    val spark = SparkHelper.getSparkSession("test","local[2]")

    spark.sparkContext.setLogLevel("WARN")
    spark.sql(
      """
        |select * from src.MGM_OPER_LOG
      """.stripMargin).show()
  }
}
