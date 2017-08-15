package com.util

import java.util.Date

import org.apache.hadoop.hbase.{Cell, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{PrefixFilter, RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by hutao on 2017/5/23.
  */
object HbaseUtil {
  private val conf = new HbaseConfig().createHbaseConfig

  /**
    * 创建Hbase连接
    * @return
    */
  def createHConnection():Connection={
    val conn = ConnectionFactory.createConnection(conf)
    conn
  }

  /**
    * 关闭Hbase连接
    * @param conn
    */
  def close(conn:Connection): Unit ={
    conn.close()
  }

  /**
    * 创建Hbase表
    * @param conn
    * @param tableName
    * @param familys
    */
  def createTable(conn: Connection,tableName:String,familys: List[String]): Unit ={
    val desc = new HTableDescriptor(TableName.valueOf(tableName))
    familys.foreach(family=>desc.addFamily(new HColumnDescriptor(family)))
    conn.getAdmin.createTable(desc)
  }


  /**
    * 删除Hbase表
    * @param conn
    * @param tableName
    */
  def dropTable(conn: Connection,tableName:String): Unit ={
    conn.getAdmin.deleteTable(TableName.valueOf(tableName))
  }

  /**
    * 禁用表
    * @param conn
    * @param tableName
    */
  def disableTable(conn:Connection,tableName:String): Unit ={
    conn.getAdmin.disableTable(TableName.valueOf(tableName))
  }

  /**
    * 启用表
    * @param conn
    * @param tableName
    */
  def enableTable(conn:Connection,tableName:String): Unit ={
    conn.getAdmin.enableTable(TableName.valueOf(tableName))
  }

  /**
    * 表是否禁用
    * @param conn
    * @param tableName
    * @return
    */
  def isTableDisabled(conn:Connection,tableName:String): Boolean ={
    conn.getAdmin.isTableDisabled(TableName.valueOf(tableName))
  }
  /**
    * 截断Hbase表
    * @param conn
    * @param tableName
    */
  def truncateTable(conn: Connection,tableName:String): Unit ={
    conn.getAdmin.truncateTable(TableName.valueOf(tableName),true)
  }

  /**
    * 判断表是否存在
    * @param tableName
    * @param conn
    * @return
    */
  def isExist(tableName:String,conn: Connection): Boolean ={
    conn.getAdmin.tableExists(TableName.valueOf(tableName))
  }


  /**
    * 批量添加数据到hbase表
    * @param conn
    * @param tableName
    * @param puts
    */
  def add(conn: Connection, tableName: String, puts:List[Put]): Unit ={
    val table = conn.getTable(TableName.valueOf(tableName))
    table.put(puts.asJava)
    table.close()
  }

  /**
    * 添加数据到hbase
    * @param conn
    * @param tableName
    * @param put
    */
  def add(conn: Connection, tableName: String,put: Put): Unit ={
    val table = conn.getTable(TableName.valueOf(tableName))
    table.put(put)
    table.close()
  }

  /**
    * 删除hbase记录
    * @param conn
    * @param tableName
    * @param delete
    */
  def del(conn: Connection, tableName: String,delete: Delete): Unit ={
    val table = conn.getTable(TableName.valueOf(tableName))
    table.delete(delete)
    table.close()
  }

  /**
    * 批量删除habse记录
    * @param conn
    * @param tableName
    * @param deletes
    */
  def del(conn: Connection, tableName: String,deletes: List[Delete]): Unit ={
    val table = conn.getTable(TableName.valueOf(tableName))
    table.delete(deletes.asJava)
    table.close()
  }

  /**
    * 获取hbase记录
    * @param conn
    * @param tableName
    * @param get
    */
  def get(conn: Connection, tableName: String,get: Get): Result ={
    val table = conn.getTable(TableName.valueOf(tableName))
    val result: Result = table.get(get)
    table.close()
    result
  }

  def getRowKey(conn: Connection, tableName: String,rowKey: String): Result ={
    get(conn,tableName,new Get(rowKey.getBytes()))
  }

  /**
    * 批量获取habse记录
    * @param conn
    * @param tableName
    * @param gets
    */
  def get(conn: Connection, tableName: String,gets: List[Get]): Array[Result] ={
    val table = conn.getTable(TableName.valueOf(tableName))
    val results: Array[Result] = table.get(gets.asJava)
    table.close()
    results
  }

  def getRowKeys(conn: Connection, tableName: String,rowKeys: List[String]): Array[Result] ={
    get(conn,tableName,rowKeys.map(row=>new Get(row.getBytes())))
  }


  /**
    * 根据前缀过滤Hbase数据
    * @param conn
    * @param tableName
    * @param prefix
    * @return
    */
  def scanWithPrefix(conn: Connection, tableName: String, prefix:String): Array[Result] ={
    val table = conn.getTable(TableName.valueOf(tableName))
    val scan = new Scan()
    scan.setBatch(100000)
    scan.setStartRow(prefix.getBytes)
    scan.setFilter(new PrefixFilter(prefix.getBytes()))
    val scanner: ResultScanner = table.getScanner(scan)
    scanner.iterator().asScala.toArray
  }

  /**
    * 正则过滤
    * @param conn
    * @param tableName
    * @param regex
    * @return
    */
  def scanWithRegex(conn: Connection, tableName: String, regex:String): Array[Result] ={
    val table = conn.getTable(TableName.valueOf(tableName))
    val comp = new RegexStringComparator(regex)
    val scan = new Scan()
    scan.setBatch(100000)
    scan.setFilter(new RowFilter(CompareOp.EQUAL,comp))
    val scanner = table.getScanner(scan)
    scanner.iterator().asScala.toArray
  }

  def scanWithStartEndRow(conn:Connection,tableName: String, startRow:String,endRow:String): Array[Result] ={
    val table = conn.getTable(TableName.valueOf(tableName))
    val scan = new Scan()
    scan.setStartRow(startRow.getBytes())
    scan.setStopRow(endRow.getBytes())
    val scanner = table.getScanner(scan)
    scanner.iterator().asScala.toArray
  }

  /**
    * 解析Result数据
    * @param result
    * @return
    */
  def parseResult(result: Result): (String, mutable.Map[String, mutable.Map[String, String]]) ={
    val rawCells: Array[Cell] = result.rawCells()
    (result.getRow.string,result.getNoVersionMap.asScala.map(m1=>
      m1._1.string -> m1._2.asScala.map(m2 =>
        m2._1.string -> m2._2.string
      )
    )
    )
  }

  def convert(map1:(String, mutable.Map[String, mutable.Map[String, String]])):java.util.Map[String, java.util.Map[String, java.util.Map[String, String]]]={
      Map(map1._1->map1._2).map(m1=>m1._1 -> m1._2.map(m2 => m2._1->m2._2.asJava).asJava).asJava
  }

  /**
    * 解析Result数据
    * @param results
    * @return
    */
  def parseResults(results:Array[Result]): Map[String, mutable.Map[String, mutable.Map[String, String]]] ={
    results.map(r=>r.getRow.string->r.getNoVersionMap.asScala.map(m1=>
      m1._1.string -> m1._2.asScala.map(m2 =>
        m2._1.string -> m2._2.string
      )
    )).toMap
  }

  def converts(map1:Map[String, mutable.Map[String, mutable.Map[String, String]]]):java.util.Map[String, java.util.Map[String, java.util.Map[String, String]]]={
    map1.map(m1=>m1._1 -> m1._2.map(m2 => m2._1->m2._2.asJava).asJava).asJava
  }


  /**
    * 构造Hbase的一行数据的Put(s)
    * @param arr 数组第一条是rowkey，其余为qualifier的value，与colType一一对应
    * @param len rowkey占用的字段数量
    * @param colType(family,List(qualifier))
    * @return
    */
  def createPuts(arr: Array[String], colType: Map[String,List[String]])(implicit len:Int=1): List[Put] = {
    val puts = new ListBuffer[Put]()
    val (rows, values) = arr.splitAt(len)
    val rowkey = rows.mkString("_")
    var i = 0
    for ((k, v) <- colType) {
      for (col <- v) {
        val put = new Put(rowkey.getBytes)
        put.addColumn(k.getBytes, col.getBytes, values(i).getBytes)
        puts += put
        i += 1
      }
    }
    puts.toList
  }

  implicit class NewArray(array: Array[Byte]){
    def string: String ={
      Bytes.toString(array)
    }
  }


  def main(args: Array[String]): Unit = {
    val conn = createHConnection()
    val start = new Date().getTime
    //    println(parseResults(scanWithStartEndRow(conn,"iUserHistorySku","9951419735_0","9951419735_9")))
    println(parseResults(scanWithPrefix(conn,"gmscm-t-LSXSSPMX","1001,1039034001")))
    val end = new Date().getTime
    println(end-start+"ms")
  }
}
