package sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLCreate {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("createSQL")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val df: DataFrame = spark.read.json("spark/in/user.json")
    df.createOrReplaceTempView("user")
    val df2: DataFrame = spark.sql("select * from user")
    df2.show()
  }

}
