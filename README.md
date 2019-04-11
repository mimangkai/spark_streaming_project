## 基于spark Streaming的实时日志统计分析项目
**需求说明**
* 当天到目前为止课程的访问量
* 当天到目前为止从搜索引擎引流过来的课程访问量

**项目环境要求**：spark、flume、kafka、hbase

**开发步骤:** \
1、使用Python脚本实时产生数据 \
见resources文件夹下的generate_log.py文件

2、使用定时调度工具每一分钟产生一批数据 \
使用linux的crontab \
从网站 www.tool.lu/crontab 学习使用 \
每一分钟执行一次的crontab表达式：*/1 * * * * <command>
command为generate.sh的全路径

3、使用Flume实时收集日志信息，输出到kafka \
采集方案resources文件夹下的streaming_project2.conf
启动flume：
```
flume-ng agent \
--name exec-memory-kafka \
--conf $FLUME_HOME/conf \
--conf-file /opt/spark_streaming_project/streaming_project2.conf \
-Dflume.root.logger=INFO,console
```
可以启动kafka消费者查看是否接受到数据：
`kafka-console-consumer.sh --zookeeper node1:2181 --topic streamingtopic`

4、使用Spark Streaming完成数据清洗和数据分析统计操作 \
从原始日志数据提取需要的字段信息，再对清洗数据进行各指标的分析统计 \
详细代码见cn.njupt.spark.ImoocStatStreamApp

5、设计hbase表，将统计结果保存到hbase中
* 功能一：统计今天到现在为止实战课程的访问量

字段：day course_id click_count

使用数据库存储统计结果

选择什么数据库存储统计结果？ 
关系型数据库更新统计结果，需要将之前的结果取出来，加上新的统计结果，再写回数据表 
HBase：提供了一个API搞定

HBase表设计 \
创建表
`create 'imooc_course_clickcount','info'`

Rowkey设计 
`day_course`

表对应实体类设计
```
case class CourseClickCount(day_course: String, click_count: Int)
```

* 功能二：统计今天到现在为止从搜索引擎引流过来的实战课程的访问量
HBase表设计

创建表 \
`create 'imooc_course_search_clickcount','info'`

Rowkey设计:根据业务需求 \
`day_search_course` 如：20171111_www.baidu.com_8

表对应实体类设计
```
case class CourseSearchClickCount(day_search_course:String, click_count:Int)
```

## 补充：
### 项目运行到服务器环境
1、编译打包 
`mvn clean package -DskipTests`

报错：
```
E:\IdeaProjects\bigdata\spark_streaming_project\src\main\scala\cn\njupt\spark\dao\CourseClickCountDAO.scala:4: error: object HBaseUtils is not a member of package
 cn.njupt.spark.utils
```
原因：HBaseUtils是一个java类

解决：在pom文件中，注释掉
```xml
<!--<sourceDirectory>src/main/scala</sourceDirectory>
    <testSourceDirectory>src/test/scala</testSourceDirectory>-->
```
2、上传jar包执行 
```
spark-submit \ 
--master yarn \ 
--class cn.njupt.spark.ImoocStatStreamApp \ 
/opt/spark_streaming_project/spark_streaming_project-1.0.jar \ 
node1:2181 test sparkstreamingtopic 1
```
报错：

原因：缺少spark-streaming-kafka-0-8_2.11依赖

解决：加--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.1

注意事项： 
1）--packages的使用 
2）--jars的使用