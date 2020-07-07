package accumulator

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

// 自定义累加器
// 1. 继承 AccumulatorV2，并设定泛型
// 2. 重写累加器的抽象方法
class WordCountAccumulator extends AccumulatorV2[String, util.ArrayList[String]]{
    var list = new util.ArrayList[String]()
    // 累加器是否为初始状态
    override def isZero: Boolean = {
        list.isEmpty
    }
    // 复制累加器
    override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
        new WordCountAccumulator()
    }
    // 重置累加器
    override def reset(): Unit = {
        list.clear()
    }
    // 向累加器中增加数据 (In)
    override def add(word: String): Unit = {
        if(word.contains("h")){
            list.add(word)
        }
    }
    // 合并累加器（多个累加器合并）
    override def merge(other: AccumulatorV2[String, util.ArrayList[String]]):Unit = {
        list.addAll(other.value)
    }
    // 返回累加器的结果 （Out）
    override def value: util.ArrayList[String] = list

}
object WordCountAccumulator{
    /**
      *统计rdd中的包含h字符的字符串，汇成集合
      * @param args
      */
    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
        //创建累加器
        val wordAccumulator = new WordCountAccumulator
        //注册累加器
        sc.register(wordAccumulator)
        //
        val rdd = sc.makeRDD(List("spark","hbase","hive","kafka"))
        rdd.foreach(
            word => {
                // 使用累加器
                wordAccumulator.add(word)
            }
        )
        // 获取累加器的值
        println("wordAccumulator= " + wordAccumulator.value)
    }
}