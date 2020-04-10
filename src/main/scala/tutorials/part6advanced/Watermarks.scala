package tutorials.part6advanced

import java.io.PrintStream
import java.net.ServerSocket
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import scala.concurrent.duration._

object Watermarks {
  val spark = SparkSession.builder()
    .appName("Watermarks")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  def testWatermarks() = {
    val dataDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        val timestamp = new Timestamp(tokens(0).toLong)
        val data = tokens(1)
        (timestamp, data)
      }
      .toDF("created", "color")

    val watermarkedDF = dataDF
      .withWatermark("created", "2 seconds")
      .groupBy(window(col("created"), "2 seconds"), col("color"))
      .count()
      .selectExpr("window.*", "color", "count")

    val query = watermarkedDF.writeStream
      .format("console")
      .outputMode(OutputMode.Append)
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    testWatermarks()
  }
}

object DataSender {
  val serverSocket = new ServerSocket()
  val socket = serverSocket.accept()
  val printer = new PrintStream(socket.getOutputStream)
}
