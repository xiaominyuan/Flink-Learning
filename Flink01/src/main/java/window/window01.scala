package window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object window01 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost",11111)
    val streamKeyBy = stream.map(x => (x,1)).keyBy(0)

    //countWindow中的size是指单个KEY的数量达到了size才会触发countwindow执行
    //滑动窗口
//    val streamWindow = streamKeyBy.countWindow(5).reduce((x1,x2) => (x1._1,x1._2+x2._2))
    val streamWindow = streamKeyBy.timeWindow(Time.seconds(5)).reduce((a,b) => (a._1,a._2+b._2))
    streamWindow.print()
    env.execute("WindowJob")
  }

}
