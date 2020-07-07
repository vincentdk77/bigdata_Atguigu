package sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQLTransferCaseClass {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transfer")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",25),(3,"wangwu",30)))
    //RDD转换成DS
    import spark.implicits._
    val rdd1: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val dataSet: Dataset[User] = rdd1.toDS()
    val frame: DataFrame = dataSet.toDF()
    frame.rdd
    val rdd2: RDD[User] = dataSet.rdd
    rdd2.foreach(println)

  }

}
