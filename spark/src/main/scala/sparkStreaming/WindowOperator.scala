package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ck")
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("linux1", 9999)
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    //注意：这两者都必须为采集周期大小的整数倍。
    // WordCount 第三版：3 秒一个批次，窗口 12 秒，滑步 6 秒。
    val wordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b),Seconds(12), Seconds(6))
    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    ssc.start()
    // Start the computation
    ssc.awaitTermination()
    // Wait for the computation to terminate
  }

}
