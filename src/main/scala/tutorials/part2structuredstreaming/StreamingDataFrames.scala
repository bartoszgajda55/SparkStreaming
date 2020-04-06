package tutorials.part2structuredstreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import common._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object StreamingDataFrames {
  val spark = SparkSession.builder()
    .appName("Streaming Data Frames")
    .master("spark://localhost:7077")
    .getOrCreate()

  def readFromSocket() = {
    // Reading DF
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // Transformations
    val shortLines: DataFrame = lines.filter(length(col("value")) <= 5)

    // Consuming DF
    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }

  def readFromFiles() = {
    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def demoTriggers = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // Write the lines DF at a certain Trigger
    lines.writeStream
      .format("console")
      .outputMode("append")
      //.trigger(Trigger.ProcessingTime(2.seconds)) // Every two seconds, run the query
      //.trigger(Trigger.Once()) // Single batch
      .trigger(Trigger.Continuous()) // Every 2 seconds, regardless of data flow
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
//    readFromSocket()
//    readFromFiles()
    demoTriggers
  }
}
