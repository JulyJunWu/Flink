package com.ws.flink.source

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @Description: 读取数据源测试
 * @Author: JulyJunWu
 * @Date: 2020/3/30 21:54
 */


object SourceTest {

  case class ShopRecord(id: String, createTime: Long, price: Double)

  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    // 获取数据源
    // val sourceData = fromCollection(environment)
    val sourceData = readTextFile(environment)
    // 打印
    sourceData.print().setParallelism(1)
    // 执行
    environment.execute()
  }

  /**
   * 从集合中获取数据源
   */
  def fromCollection(environment: StreamExecutionEnvironment): DataStream[ShopRecord] = {
    environment.fromCollection(List(
      ShopRecord("1", System.currentTimeMillis(), 9.9),
      ShopRecord("2", System.currentTimeMillis(), 19.9),
      ShopRecord("3", System.currentTimeMillis(), 29.9),
      ShopRecord("4", System.currentTimeMillis(), 39.9),
      ShopRecord("5", System.currentTimeMillis(), 49.9)
    ))
  }

  /**
   * 从元素获取数据源
   */
  def fromElements(environment: StreamExecutionEnvironment): DataStream[Object] = {
    environment.fromElements("good", "Nice", new Integer(1))
  }

  /**
   * 从文件中获取数据源
   */
  def readTextFile(environment: StreamExecutionEnvironment): DataStream[String] = {
    environment.readTextFile("D:\\workspace\\Flink\\src\\main\\resource\\hello.txt")
  }


}
