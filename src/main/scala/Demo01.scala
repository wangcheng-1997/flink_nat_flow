//import org.apache.flink.streaming.api.scala._
import java.util

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
//import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable
object Demo01 {

  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

  def main(args: Array[String]): Unit = {

    //TODO 1.0 流api wordcount
//    streamWordCount
    //TODO 2.0 批api wordcount
//      batchWordCount
val value = new util.HashMap[String,String]()
    value.put("01","chengg")
    value.put("02","wongx")
    value.put("03","lixian")
    println(value.toString)

  }

//  def streamWordCount: Unit ={
//    // 获取执行器的环境
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//
//    //获取数据: 从socket中获取
//    val textDataStream = env.socketTextStream("30.250.60.3", 6363, '\n')
//    val tupDataStream = textDataStream.flatMap(_.split(" ")).map(WordWithCount(_,1))
//
//    //    //groupby: 按照指定的字段聚合
//    val windowDstram = tupDataStream.keyBy("word").timeWindow(Time.seconds(5), Time.seconds(5))//窗口bsize=5秒, slid=1s
//    windowDstram.sum("count").print()
//
//    //启动执行器，执行程序
//    env.execute()
//  }

  def batchWordCount: Unit ={
    // 获取执行器的环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //获取数据: 从文本中获取
    val textData = env.readTextFile("file:///E:\\workspace\\flink\\study\\home\\test.txt","utf8")
    val tupData: DataSet[WordWithCount] = textData.flatMap(_.split(" "))
      .map(WordWithCount(_,1))

    //    //groupby: 按照指定的字段聚合
    val window = tupData
      .groupBy("word")

    window.sum("count").writeAsText("file:///E:\\workspace\\flink\\study\\home\\result.txt",WriteMode.OVERWRITE)


    //启动执行器，执行程序
    env.execute()
  }

}
