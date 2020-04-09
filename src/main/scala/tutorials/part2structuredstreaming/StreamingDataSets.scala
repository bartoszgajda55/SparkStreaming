package tutorials.part2structuredstreaming

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import common._

object StreamingDataSets {
  val spark = SparkSession.builder()
    .appName("Streaming DataSets")
    .master("local[1]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  import spark.implicits._

  def readCars(): Dataset[Car] = {
    val carEncoder = Encoders.product[Car]
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), carsSchema).as("car")) // composite column (struct)
      .selectExpr("car.*") // DF with multiple columns
      .as[Car] // encoder can be passed explicitly
      //.as[Car](carEncoder) // encoder can be passed explicitly
  }

  def showCarNames() = {
    val carsDS: Dataset[Car] = readCars()

    val carNamesDF = carsDS.select(col("name")) // DF

    // collection transformations maintain type info
    val carNamesAlt = carsDS.map(_.Name) // DS[String]

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def ex1() = {
    val carsDS = readCars()
    carsDS.filter(_.Horsepower.getOrElse(0L) > 140)
  }

  def ex2() = {
    val carsDS = readCars()
    carsDS.select(avg(col("Horsepower")))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def ex3() = {
    val carsDS = readCars()

    val carCountByOrigin = carsDS.groupBy(col("Origin")).count() // DF API
    val carCountByOriginAlt = carsDS.groupByKey(car => car.Origin).count() // DS API

    carCountByOriginAlt.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    showCarNames()
  }
}
