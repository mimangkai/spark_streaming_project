package cn.njupt.spark.dao

import cn.njupt.spark.domain.{CourseClickCount, CourseSearchClickCount}
import cn.njupt.spark.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 从搜索引擎过来的实战课程点击数-数据访问层
  */
object CourseSearchClickCountDAO {
  val tableName = "imooc_course_search_clickcount"
  val cf = "info"
  val qualifer = "click_count"

  /**
    * 保存数据到HBase
    *
    * @param list CourseSearchClickCount集合
    */
  def save(list: ListBuffer[CourseSearchClickCount]) = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    for (elem <- list) {
      table.incrementColumnValue(Bytes.toBytes(elem.day_search_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        elem.click_count)
    }
  }

  /**
    * 根据rowkey查询值
    *
    * @param day_course
    * @return
    */
  def count(day_course: String): Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_course))
    val value = table.get(get).getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifer))
    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CourseSearchClickCount]
    list.append(CourseSearchClickCount("20171111_www.baidu.com_8",8))
    list.append(CourseSearchClickCount("20171111_cn.bing.com_9",9))

    save(list)

    println(count("20171111_www.baidu.com_8") + " : " + count("20171111_cn.bing.com_9"))
  }
}
