package sparkSQL

import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql._

object SparkSQL_UDAF_strongType {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("udaf").getOrCreate()
    //创建用户定义聚合对象（强类型），不需要注册类
    val udaf = new MyAgeAvgClassFunction
    val avgCol: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgAge")
    //使用udaf
    val df: DataFrame = spark.read.json("spark/in/user.json")
    import spark.implicits._
    val dataSet: Dataset[UserBean] = df.as[UserBean]
    //DSL语法
    //scala> df.select("username").show()
    val resultDS: Dataset[Double] = dataSet.select(avgCol)
    resultDS.show()
    spark.stop()

  }

}

case class UserBean(name:String,age:BigInt)  //读文件的时候类型比较大 ，默认是BigInt，如果用int会报错（无法将BigInt转成int类型）
case class AvgBuffer(var sum:BigInt,var count:Int)
//用户自定义聚合函数类(强类型 strong type)  推荐使用！！！！
//1、继承aggregator，设定泛型
//2、实现方法
class MyAgeAvgClassFunction extends Aggregator[UserBean,AvgBuffer,Double]{
  override def zero: AvgBuffer = {
    AvgBuffer(0,0)
  }

  /**
    * 聚合数据
    * @param b
    * @param a
    * @return
    */
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum += a.age
    b.count += 1
    b
  }

  /**
    * 缓冲区的合并操作
    * @param b1
    * @param b2
    * @return
    */
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  /**
    * 完成计算
    * @param reduction
    * @return
    */
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  /**
    * 固定形式
    * @return
    */
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}