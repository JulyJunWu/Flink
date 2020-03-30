package com.ws.flink.wc

import org.apache.flink.streaming.api.scala._

/**
 * @Description: 流处理 wc
 * @Author: JulyJunWu
 * @Date: 2020/3/29 22:21
 */
object StreamWorldCount {

  def main(args: Array[String]): Unit = {
    // 创建流处理环境对象
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //监听socket源
    val datasource = environment.socketTextStream(args(0), args(1).toInt)
    // 处理及获取结果
    val result = datasource.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    // 设置并行度,结果打印
    result.print()
    //提交任务 执行并不断监听数据以及处理
    environment.execute(StreamWorldCount.getClass.getSimpleName)
  }
}
