package com.ws.flink

import org.apache.flink.api.scala._

/**
 * @Description: 批处理(离线) wc
 * @Author: JulyJunWu
 * @Date: 2020/3/29 21:36
 */
object WorldCount {

  def main(args: Array[String]): Unit = {
    //创建环境
    val environment = ExecutionEnvironment.getExecutionEnvironment
    //读取数据源
    val datasource = environment.readTextFile("D:\\workspace\\Flink\\src\\main\\resource\\hello.txt")
    //数据处理
    val result = datasource.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    // 结果打印
    result.print()
  }
}
