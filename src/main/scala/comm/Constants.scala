package comm

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf

object Constants {

  val NAME = "etl_mgm_oper_log"
  val MASTER = "yarn"

  val SECONDS = 10
  val LOG_LEVEL = "WARN"


  //  val KAFKA_SERVERS = "10.20.1.109:9092"
  val KAFKA_SERVERS = "10.20.12.132:9092,10.20.12.133:9092,10.20.12.134:9092"
  //  val GROUP_ID = "etl_mgm_oper_log"
  //恢复9月1号到现在的数据
  val GROUP_ID = "etl_mgm_oper_log"
  val EARLIEST = "earliest"
  val LATEST = "latest"
  //  val TOPIC = "se-do-data-mh-preonline"
  val TOPIC = "se-do-data-xgw-online"


  //存储offset的mysql地址
  //  val MYSQL_URL = "jdbc:mysql://10.12.40.206:3306/test?characterEncoding=UTF-8"
  val MYSQL_URL = "jdbc:mysql://10.20.2.245:3306/rtm_server?characterEncoding=UTF-8"
  val USER = "mycat"
  val PASSWORD = "12345678"


  //匹配页面url
  val PRODUCT_LIST = List(
    //# product.xml 产品 appTag=2
    "/manager-webapi/product/productInformation",
    "/manager-webapi/product/productStream",
    "/manager-webapi/product/productAttribute",
    "/manager-webapi/product/appCategory",
    "/manager-webapi/product/productMark",
    "/manager-webapi/product/productShowcase"
  )


  val CONTENT_LIST = List(
    "/manager-webapi/content/atlas/",
    "/manager-webapi/content/atlascategory/",
    "/manager-webapi/interaction/question/",
    "/manager-webapi/content/info/",
    "/manager-webapi/content/infoCategory/",
    "/manager-webapi/content/introcate/",
    "/manager-webapi/content/introduction/",
    "/manager-webapi/interaction/staff/",
    "/manager-webapi/interaction/staffGroup/",
    "/manager-webapi/content/mapPostion/",
    "/manager-webapi/content/mapCategory/",
    "/manager-webapi/content/mapCard/",
    "/manager-webapi/interaction/jobposting/",
    "/manager-webapi/interaction/jobpostingDepartment/",
    "/manager-webapi/content/companyFileCategory/",
    "/manager-webapi/content/companyFile/",
    "/manager-webapi/content/companyfile/",
    "/manager-webapi/content/corpvideo/category/",
    "/manager-webapi/content/corpvideo/",
    "/manager-webapi/content/corpvideo/ajax/",
    "/manager-webapi/content/banner/",
    "/manager-webapi/content/navagation/",
    "/manager-webapi/content/complaintPageContent/",
    "/manager-webapi/interaction/toolbarBtn/",
    "/manager-webapi/interaction/toolbar/",
    "/dssresources/imageRepository/",
    "/dssresources/fileRepository/",
    "/dssresources/videoRepository/",
    "/manager-webapi/repository/config/",
    "/manager-webapi/repository/code/",
    "/manager-webapi/repository/rule/",
    "/manager-webapi/interaction/seoRule/",
    "/manager-webapi/interaction/messageboard/",
    "/manager-webapi/content/friendshiplink/",
    "/manager-webapi/content/friendshiplinkCategory/"
  )

  //租户ID的验证 数据库
  val SITE_USER = ""
  val SITE_PASSWORD = ""
  val SITE_TABLE = ""
  val SITE_URL = ""

  val sql =
    """
      |insert into src.MGM_OPER_LOG partition (day)
      |select
      |id,
      |log_id ,
      |login_name ,
      |login_ip  ,
      |user_browser_name  ,
      |user_browser_version ,
      |user_os_name  ,
      |user_login_device  ,
      |create_date_time  ,
      |oper_content  ,
      |request_parameters  ,
      |tid,
      |tag,
      |day
      |from tmp
    """.stripMargin

  val sql2 =
    """
      |insert into src.MGM_OPER_LOG2 partition (day)
      |select
      |id,
      |log_id ,
      |login_name ,
      |login_ip  ,
      |user_browser_name  ,
      |user_browser_version ,
      |user_os_name  ,
      |user_login_device  ,
      |create_date_time  ,
      |oper_content  ,
      |request_parameters  ,
      |tid,
      |tag,
      |day
      |from tmp
    """.stripMargin

}
