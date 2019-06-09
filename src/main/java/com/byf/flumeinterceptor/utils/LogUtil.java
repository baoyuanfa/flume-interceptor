package com.byf.flumeinterceptor.utils;

import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUtil {

    //获取日志工厂
    private static Logger logger = LoggerFactory.getLogger(LogUtil.class);

    public static Boolean isValidate(String log){


        try {
            //切分log
            String[] logContext = log.split("\\|");

            //判断log长度是否有效
            if (logContext.length < 2) {
                return false;
            }

            //判断日期是否为13位有效数字
            if (logContext[0].length() != 13 || NumberUtils.isDigits(logContext[0])) {
                return false;
            }

            //判断日志Json格式是否完整
            if (!logContext[1].trim().startsWith("{") && logContext[1].trim().endsWith("}")) {
                return false;
            }
        } catch (Exception e) {
            //如果出错，打印错误信息
            logger.error("error log message is : " + log);
            logger.error(e.getMessage());
        }

        return true;
    }
}
