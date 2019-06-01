package source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkSource01 {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //获取数据源（source）
//    val stream = env.readTextFile("test00.txt")
    val stream = env.socketTextStream("localhost",11111)
    //打印数据
    stream.print()
    //执行任务
    env.execute("FirstJob")

  }

}
