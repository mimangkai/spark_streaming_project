package cn.njupt.spark

import cn.njupt.spark.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import cn.njupt.spark.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import cn.njupt.spark.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object ImoocStatStreamApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.print("usage:ImoocStatStreamApp <zkQuorum> <groupId> <topics> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics, numThreads) = args

    val conf = new SparkConf().setAppName("ImoocStatStreamApp")
    //.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(60))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

    //测试步骤一：测试数据接受
    //messages.map(_._2).count().print

    //测试步骤二：数据清洗
    val logs = messages.map(_._2)
    val cleanData = logs.map(line => {
      val infos = line.split("\t")
      val url = infos(2).split(" ")(1)
      var courseId = 0

      //只取实战课程的课程编号
      //156.72.187.10   2019-04-09 20:08:01     "GET /class/131.html HTTP/1.1"  200     -
      if (url.startsWith("/class")) {
        val courseIdHtml = url.split("/")(2)
        courseId = courseIdHtml.substring(0, courseIdHtml.indexOf(".")).toInt
      }
      ClickLog(infos(0), DateUtils.parse(infos(1)), courseId, infos(3).toInt, infos(4))
    }).filter(clicklog => clicklog.courseId != 0)

    //cleanData.print(10)

    //测试步骤三：统计今天到现在为止实战课程的访问量，写入到HBase数据表
    cleanData.map(x => {
      // HBase rowkey设计： 20171111_88
      (x.time.substring(0, 8) + "_" + x.courseId, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseClickCount(pair._1, pair._2))
        })
        CourseClickCountDAO.save(list)
      })
    })

    //测试步骤四：统计今天到现在为止从搜索引擎引流过来的实战课程的访问量
    cleanData.map(x => {
      //https://www.sogou.com/web?query=Spark SQL实战 ==> https:/www.sogou.com/web?query=Spark SQL实战
      val splits = x.referer.replaceAll("//", "/").split("/")
      var host = ""
      if (splits.length > 2) {
        host = splits(1)
      }
      (x.time, host, x.courseId)
    }).filter(_._2 != "").map(x => {
      (x._1.substring(0, 8) + "_" + x._2 + "_" + x._3, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseSearchClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseSearchClickCount(pair._1, pair._2))
        })
        CourseSearchClickCountDAO.save(list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
