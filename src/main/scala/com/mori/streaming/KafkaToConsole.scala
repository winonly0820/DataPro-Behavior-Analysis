package com.mori.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object KafkaToConsole {

  def main(args: Array[String]): Unit = {

    val kafkaServers = if (args.length > 0) args(0) else "node1:9092"
    val topicName = if (args.length > 1) args(1) else "mock_order_topic"
    val checkpointPath = if (args.length > 2) args(2) else "/tmp/checkpoint/mock_order_console"

    val spark = SparkSession.builder()
      .appName("KafkaToConsole")
      .master("local[2]")
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "1g")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.default.parallelism", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val schema = new StructType()
      .add("mock_batch_id", StringType, nullable = true)
      .add("mock_create_time", StringType, nullable = true)
      .add("event_id", StringType, nullable = true)
      .add("phone_no", StringType, nullable = true)
      .add("orderno", StringType, nullable = true)
      .add("sm_name", StringType, nullable = true)
      .add("business_name", StringType, nullable = true)
      .add("owner_name", StringType, nullable = true)
      .add("owner_code", StringType, nullable = true)
      .add("prodname", StringType, nullable = true)
      .add("offerid", StringType, nullable = true)
      .add("offername", StringType, nullable = true)
      .add("prodprcid", StringType, nullable = true)
      .add("prodprcname", StringType, nullable = true)
      .add("prodstatus", StringType, nullable = true)
      .add("run_name", StringType, nullable = true)
      .add("offertype", StringType, nullable = true)
      .add("mode_time", StringType, nullable = true)
      .add("optdate", StringType, nullable = true)
      .add("effdate", StringType, nullable = true)
      .add("expdate", StringType, nullable = true)
      .add("orderdate", StringType, nullable = true)
      .add("cost", DoubleType, nullable = true)

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", topicName)
      .option("startingOffsets", "earliest")
      .load()

    val orderDF = kafkaDF
      .selectExpr("CAST(value AS STRING) AS json_str")
      .select(from_json(col("json_str"), schema).alias("data"))
      .select("data.*")
      .filter(col("event_id").isNotNull)
      .filter(col("mock_batch_id").isNotNull)
      .filter(col("cost").isNotNull)

    val statDF = orderDF
      .groupBy(col("mock_batch_id"))
      .agg(
        count("*").alias("order_count"),
        round(sum("cost"), 2).alias("sales_amount"),
        round(avg("cost"), 2).alias("avg_cost")
      )
      .orderBy(col("mock_batch_id"))

    val query = statDF.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "50")
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}