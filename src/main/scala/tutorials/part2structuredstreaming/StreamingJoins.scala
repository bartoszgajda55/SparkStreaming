package tutorials.part2structuredstreaming

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object StreamingJoins {
  val spark = SparkSession.builder()
    .appName("Streaming Aggregations")
    .master("local[1]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  val guitarPlayers = spark.read
      .option("inferSchema", true)
    .json("src/main/resources/data/guitarPlayers")
  val guitars = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitars")
  val bands = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/bands")

  val joinCondition = guitarPlayers.col("band") === bands.col("id")
  val guitaristsBands = guitarPlayers.join(bands, joinCondition, "inner")

  val bandsSchema = bands.schema

  def joinStreamWithStatic() = {
    val streamedBandsDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // a DF with single column "value" of type String
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val streamedBandGuitaristsDF = streamedBandsDF.join(guitarPlayers, guitarPlayers.col("band") === streamedBandsDF.col("id"), "inner")

    /*
    restricted joins:
      - stream joining with static: Right outer join/full outer join/right_semi not permitted
      - static joining with streaming: Left outer join/full/left_semi not permitted
     */
    streamedBandGuitaristsDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /*
  stream to stream joins:
    - inner joins are supported
    - left/right outer joins are supported but must use watermarks
    - full outer joins are not supported
   */

  def main(args: Array[String]): Unit = {
    joinStreamWithStatic()
  }
}
