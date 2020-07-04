package partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

class CustomerPartitioner(numParts:Int) extends Partitioner{
  //获取分区数
  override def numPartitions: Int = numParts
  //根据key获取分区号
  override def getPartition(key: Any): Int = {
    val ckey: String = key.toString
    ckey.substring(ckey.length-1).toInt%numParts
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf())
    val rdd: RDD[(String, String)] = sc.makeRDD(List(("key1", "value1"), ("key2", "value2"), ("key3", "value3"), ("key4", "value4")))
//    val rdd2 = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)
    val partitionRdd: RDD[(String, String)] = rdd.partitionBy(new CustomerPartitioner(10))
    partitionRdd.foreach{case (a,b)=>{
      println(a)
    }}


  }
}