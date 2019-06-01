package transformation

import org.apache.flink.streaming.api.scala._

object Transformation02 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("text00").flatMap(x => x.split(" ")).map(x => (x,1))
    val streamKeyBy = stream.keyBy(0)
    //同一个KEY的数据聚合到一个partition, redistributed
    //reduce  合并当前的元素和上次聚合的结果， 产生一个新的值， 返回的流中包含每一次聚合的结果， 而不是只
    //返回最后一次聚合的最终结果
    val streamCount = streamKeyBy.reduce((x1, x2) => (x1._1,x1._2+x2._2))
    streamCount.print()
  }

}
