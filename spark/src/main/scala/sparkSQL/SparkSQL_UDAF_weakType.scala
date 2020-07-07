package sparkSQL

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

object SparkSQL_UDAF_weakType {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("udaf").getOrCreate()
    //创建用户定义聚合对象
    val udaf = new MyAgeAvgFunction
    spark.udf.register("avgAge",udaf)
    //使用udaf
    val df: DataFrame = spark.read.json("spark/in/user.json")
    df.createOrReplaceTempView("user")
    val frame: DataFrame = spark.sql("select avgAge(age) from user")
    frame.show()
    spark.stop()
  }
}

//用户自定义聚合函数类(弱类型 weak type)
//弱类型，必须根据顺序来传，如果记错了顺序就会出错！推荐使用强类型
class MyAgeAvgFunction extends UserDefinedAggregateFunction{
  //函数输入的数据结构
  override def inputSchema: StructType = {
    new StructType().add("age",LongType)
  }

  //计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)
  }

  //函数返回的数据类型
  override def dataType: DataType = DoubleType

  //函数是否稳定（入参相同返回值是否一样）
  override def deterministic: Boolean = true

  //计算前的缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0L  //第一个结构  sum
    buffer(1)=0L  //第二个结构  count
  }

  //根据查询结构来更新缓冲区
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //sum
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    //count
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
