package eventTimeWindow


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object EventTimeWindow01 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("localhost",11111).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(0)) {
        override def extractTimestamp(t: String): Long = {
          //eventTime word
          val eventTime = t.split(" ")(0).toLong
          print(eventTime)
          return eventTime
        }
      }).map(x => (x.split(" ")(1),1)).keyBy(0)

    val streamWindow = stream.window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
    val streamReduce = streamWindow.reduce((a,b) => (a._1, a._2+b._2))

    streamReduce.print()

    env.execute("EvenetTimeJob")

  }
}
