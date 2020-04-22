package demo

import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TrendingHashtags {
  def getCurrentFormatTime: String = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss").format(new Date)

  def saveDataFrameToGS(df: DataFrame, currentTime: String): Unit = {
    df.write.parquet("%s - %s.parquet".format("gs://ssu-spark/output_data/", currentTime))
    println("the original data is recorded to google storage: " + currentTime)
  }

  def writingResult(dataFrame: DataFrame, table: String): Unit = {
    dataFrame.write
      .format("bigquery")
      .option("table", table)
      .option("temporaryGcsBucket", "hikes_temp_segment")
      .mode("append")
      .save()
  }

  def top10FastSpeed(dataFrame: DataFrame, currentTime: String): Unit = {
    val res = dataFrame
      .filter(col("moving_time") > 0)
      .sort(desc("max_speed"))
      .limit(10).withColumn("time", functions.lit(currentTime))
      .select("max_speed", "time")

    writingResult(res, "hike_track_dataset.absolute_speed")

    println("top10 by FASTEST speed are recorded into database: " + currentTime)
  }

  def top10AvgSpeed(df: Dataset[Row], currentTime: String): Unit = {
    val res = df
      .filter(col("moving_time") > 0)
      .withColumn("avg_speed", col("length_2d")./(col("moving_time")))
      .select("avg_speed")
      .sort(desc("avg_speed"))
      .withColumn("time", functions.lit(currentTime))
      .limit(10)

    writingResult(res, "hike_track_dataset.average_speed")

    println("top10 by AVERAGE speed are recorded into database: " + currentTime)
  }

  def maxMinAvgMedTime(df: Dataset[Row], spark: SparkSession, currentTime: String): Unit = {
    df
      .filter(col("moving_time") > 0)
      .registerTempTable("table")

    val res = spark.sqlContext.sql("select " +
      "max(moving_time) as max, " +
      "min(moving_time) as min, " +
      "avg(moving_time) as avg, " +
      "percentile(moving_time, 0.5) as median from table")

    writingResult(res, "hike_track_dataset.hikes_statistics");

    println("max min avg med by time are recorded into database: " + currentTime)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkApp")
    val ssc = new StreamingContext(conf, Seconds(8))
    val spark = SparkSession.builder.getOrCreate

    val stream = PubsubUtils.createStream(
      ssc,
      "bold-vial-274800",
      None,
      "tracks-subscription",
      SparkGCPCredentials.builder.build(),
      StorageLevel.MEMORY_AND_DISK_SER_2)
      .map(message => new String(message.getData(), StandardCharsets.UTF_8))

    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        println("dataframe is loaded\n")

        val dataFrame = spark.read.json(rdd)
        val currentTime = getCurrentFormatTime

        saveDataFrameToGS(dataFrame, currentTime)
        top10FastSpeed(dataFrame, currentTime)
        top10AvgSpeed(dataFrame, currentTime)
        maxMinAvgMedTime(dataFrame, spark, currentTime)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}