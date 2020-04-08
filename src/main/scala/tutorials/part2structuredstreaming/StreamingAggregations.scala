package tutorials.part2structuredstreaming

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object StreamingAggregations {
  val spark = SparkSession.builder()
    .appName("Streaming Aggregations")
    .master("local[1]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  def streamingCount() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lineCount: DataFrame = lines.selectExpr("count(*) as lineCount")

    lineCount.writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggs without watermark
      .start()
      .awaitTermination()
  }

  def numericalAggregations(): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val numbersDF = lines.select(col("value").cast("integer").as("number"))
    val sumDF = numbersDF.select(sum(col("number").as("agg_so_far")))

    sumDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    streamingCount()
  }
}
