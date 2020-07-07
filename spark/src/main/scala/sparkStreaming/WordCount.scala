package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkStreaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))
    val queue = new mutable.Queue[RDD[String]]()
    val queueDS: InputDStream[String] = ssc.queueStream(queue)
    queueDS.print()


    //启动采集器
    ssc.start()
    for(i <- 1 to 100){
      val rdd: RDD[String] = ssc.sparkContext.makeRDD(List(i.toString))
      queue.enqueue(rdd)
      Thread.sleep(1000)

    }
    //driver等待采集器结束才结束，否则就一直运行
    ssc.awaitTermination()

  }

}
