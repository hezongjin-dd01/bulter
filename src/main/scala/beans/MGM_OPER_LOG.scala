package beans

case class MGM_OPER_LOG(
                         id: String,
                         log_id: String,
                         login_name: String,
                         login_ip: String,
                         user_browser_name: String,
                         user_browser_version: String,
                         user_os_name: String,
                         user_login_device: String,
                         create_date_time: String,
                         oper_content: String,
                         request_parameters: String,
                         tid: String,
                         tag: String,
                         day: String
                       )
