package cn.njupt.spark.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Discription Hbase操作工具类:Java工具类建议采用单例模式封装
 * @Author wangkai
 * @Date 2019/4/10 8:16
 * @Version 1.0
 **/
public class HBaseUtils {
    Configuration conf = null;
    Connection conn = null;

    /**
     * 私有构造方法，创建连接
     */
    private HBaseUtils() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        conf.set("hbase.rootdir", "hdfs://node1:9000/hbase");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 单例模式-懒汉模式
     */
    private static HBaseUtils instance = null;

    public static synchronized HBaseUtils getInstance() {
        if (instance == null) {
            instance = new HBaseUtils();
        }
        return instance;
    }

    /**
     * 根据表名获取到Table实例
     * @param tableName
     * @return
     */
    public Table getTable(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 向HBase表添加记录
     * @param tableName    HBase表名
     * @param rowkey       HBase表的rowkey
     * @param columnFamily HBase表的columnfamily
     * @param column       HBase表的列
     * @param value        写入HBase表的值
     */
    public void put(String tableName, String rowkey, String columnFamily, String column, String value) {
        Table table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        try {
            table.put(put);
            table.close();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        Table table = HBaseUtils.getInstance().getTable("imooc_course_clickcount");
//        System.out.println(table.getName().getNameAsString());

        String tableName = "imooc_course_clickcount" ;
        String rowkey = "20171111_88";
        String columnfamily = "info" ;
        String column = "click_count";
        String value = "2";

        HBaseUtils.getInstance().put(tableName,rowkey,columnfamily,column,value);
    }
}
