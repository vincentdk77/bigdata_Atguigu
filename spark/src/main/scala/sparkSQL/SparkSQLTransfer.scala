package sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQLTransfer {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transfer")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",25),(3,"wangwu",30)))
    //RDD转换成DF
    import spark.implicits._
    val df: DataFrame = rdd.toDF("id","name","age")
    df.show()
    //DF转换成DS
    val dataSet: Dataset[User] = df.as[User]
    dataSet.show()
    //DS转DF
    val df2: DataFrame = dataSet.toDF()
      //注意df转成rdd 每一行变成了ROW!!!!
//    取值方式一
    df2.foreach(row=>{
      val name: String = row.getAs[String]("name")
      println(name)
    })
//    取值方式二
    val rdd1: RDD[Row] = df2.rdd
    rdd1.foreach(row=>{
      println(row.getInt(0)+","+row.getString(1)+","+row.getInt(2))
    })

  }

}
case class User(id:Int,name:String,age:Int)
