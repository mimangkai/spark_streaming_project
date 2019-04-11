package cn.njupt.spark.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 时间日期工具类
  */
object DateUtils {
  val input_time_format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val target_time_format = FastDateFormat.getInstance("yyyyMMddHHmmss")

  /**
    * 将String类型的日期转换成long类型的时间戳
    *
    * @param time
    * @return
    */
  def getTime(time: String) = {
    input_time_format.parse(time).getTime
  }

  /**
    * 转换日期格式
    * @param time
    * @return
    */
  def parse(time: String) = {
    target_time_format.format(new Date(getTime(time)))
  }

  /**
    * 测试
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val testTime = "2019-04-09 20:08:01"
    println(parse(testTime))
  }
}
