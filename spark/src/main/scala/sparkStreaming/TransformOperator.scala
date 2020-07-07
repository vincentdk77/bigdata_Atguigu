package sparkStreaming

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object TransformOperator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkStreaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))
    //3.定义 Kafka 参数
    val paraMap: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "linux1:9092,linux2:9092,linux3:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu" )
    //4.读取 Kafka 数据
    val kafkaDStream: InputDStream[(String, String)] =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, paraMap, Set("atguigu"))

    //这里的代码执行一次，在driver端
    kafkaDStream.transform(rdd=>{
      //todo 这里的代码执行N次，周期执行在driver端 ！！！！！！！！！！！！！！！！！！！
      rdd.map{case (key,value)=>{
        //这里的代码执行N次，在Executor端
        println(key)
      }}
    })

    //这里的代码执行一次，在driver端
    kafkaDStream.map{
      case (key,value)=>{
        //这里的代码执行N次，在Executor端
        println(key)
      }
    }
  }

}
