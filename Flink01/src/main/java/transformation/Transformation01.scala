package transformation

import org.apache.flink.streaming.api.scala._

object Transformation01 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    val stream = env.generateSequence(1,10)

//    val stream = env.readTextFile("text00.txt")

    val streamFlat = stream.flatMap(x => x.split(" "))

    val streamMap = streamFlat.map(x => (x,1))

//    val streamCount = streamMap.reduceByKey((a,b) => a+b)

//    val streamMap = stream.map(x => x*2)

//    connect操作
//    val stream01 = env.generateSequence(1,10)
//    val stream02 = env.readTextFile("text00.txt").flatMap(x => x.split(" ")).filter(x => x=="yuan")
//    val streamConn = stream01.connect(stream02)
//    val streamCoMap = streamConn.map(x => println(x), x => println(x))

    //split select 操作
//    val stream = env.readTextFile("text00.txt").flatMap(x => x.split(" "))
//    val streamSplit = stream.split(
//      word =>
//        ("hadoop".equals(word)) match {
//          case true => List("hadoop")
//          case false => List("others")
//        }
//    )
//
//    val streamSelect01 = streamSplit.select("hadoop")
//    val streamSelect02 = streamSplit.select("other")


//    streamMap.print()

    val stream1 = env.readTextFile("text00")
    val stream2 = env.readTextFile("text01")
    val streamUnion = stream1.union(stream2)
    streamUnion.print()

    env.execute("FirstJob")
  }
}
