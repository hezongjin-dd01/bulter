package etl

import comm.Constants
import utils.{DateHelper, SparkHelper}

object etl_merge_partition {

  def main(args: Array[String]): Unit = {


    val spark = SparkHelper.getSparkSession("merge_partition", "yarn")

    spark.sparkContext.setLogLevel("WARN")

    val yesterday = DateHelper.getYesterday


    //合并小文件
    spark.sql(
      s"""
         |insert overwrite table src.MGM_OPER_LOG partition (day='${yesterday}')
         |select
         |tenant_id ,
         |log_id ,
         |login_name ,
         |login_ip ,
         |user_browser_name ,
         |user_browser_version ,
         |user_os_name ,
         |user_login_device ,
         |create_date_time ,
         |oper_content ,
         |request_parameters ,
         |tid,
         |tag
         |from src.MGM_OPER_LOG
         |where day ='${yesterday}'
      """.stripMargin).show()



    //统计数据
    spark.sql(
      s"""
         |insert overwrite table src.t_src_portal_statistics_new partition (extract_date='${yesterday}')
         |select
         |tenant_id,
         |sum(case when tag="product" then 1 else 0 end) as about_product,
         |sum(case when tag="content" then 1 else 0 end) as about_content
         |from src.MGM_OPER_LOG
         |where day='${yesterday}'
         |group by tenant_id
        """.stripMargin).show()

  }

}
