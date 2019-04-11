package cn.njupt.spark.dao

import cn.njupt.spark.domain.CourseClickCount
import cn.njupt.spark.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 实战课程点击数-数据访问层
  */
object CourseClickCountDAO {
  val tableName = "imooc_course_clickcount"
  val cf = "info"
  val qualifer = "click_count"

  /**
    * 保存数据到HBase
    *
    * @param list CourseClickCount集合
    */
  def save(list: ListBuffer[CourseClickCount]) = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    for (elem <- list) {
      table.incrementColumnValue(Bytes.toBytes(elem.day_course),
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
    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20171111_8",8))
    list.append(CourseClickCount("20171111_9",9))
    list.append(CourseClickCount("20171111_1",100))

    save(list)
    println(count("20171111_8") + " : " + count("20171111_9")+ " : " + count("20171111_1"))
  }
}
