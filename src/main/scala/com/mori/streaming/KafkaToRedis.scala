package com.mori.streaming

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import redis.clients.jedis.Jedis

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.HashMap

object KafkaToRedis {

  def main(args: Array[String]): Unit = {

    val kafkaServers = if (args.length > 0) args(0) else "node1:9092"
    val topicName = if (args.length > 1) args(1) else "mock_order_topic"
    val redisHost = if (args.length > 2) args(2) else "127.0.0.1"
    val redisPort = if (args.length > 3) args(3).toInt else 6379
    val checkpointPath = if (args.length > 4) args(4) else "/tmp/checkpoint/mock_order_redis"

    val spark = SparkSession.builder()
      .appName("KafkaToRedis")
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
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", checkpointPath)
      .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>

        if (batchDF.take(1).nonEmpty) {

          println("======================================")
          println(s"Spark Streaming batchId = $batchId")
          println("当前统计结果：")
          batchDF.show(false)

          val jedis = new Jedis(redisHost, redisPort)

          try {
            val rows = batchDF.collect()

            var totalOrderCount = 0L
            var totalSalesAmount = 0.0

            var latestBatchId = ""
            var latestBatchOrderCount = 0L
            var latestBatchSalesAmount = 0.0
            var latestBatchAvgCost = 0.0

            rows.foreach { row =>
              val mockBatchId = row.getAs[String]("mock_batch_id")
              val orderCount = row.getAs[Long]("order_count")
              val salesAmount = row.getAs[Double]("sales_amount")
              val avgCost = row.getAs[Double]("avg_cost")

              totalOrderCount += orderCount
              totalSalesAmount += salesAmount

              if (latestBatchId == "" || mockBatchId > latestBatchId) {
                latestBatchId = mockBatchId
                latestBatchOrderCount = orderCount
                latestBatchSalesAmount = salesAmount
                latestBatchAvgCost = avgCost
              }

              val batchKey = s"mock_order:batch:$mockBatchId"

              val batchMap = new HashMap[String, String]()
              batchMap.put("mock_batch_id", mockBatchId)
              batchMap.put("order_count", orderCount.toString)
              batchMap.put("sales_amount", salesAmount.toString)
              batchMap.put("avg_cost", avgCost.toString)
              batchMap.put("update_time", currentTime())

              jedis.hset(batchKey, batchMap)
            }

            val totalAvgCost =
              if (totalOrderCount == 0) 0.0
              else BigDecimal(totalSalesAmount / totalOrderCount)
                .setScale(2, BigDecimal.RoundingMode.HALF_UP)
                .toDouble

            val dashboardMap = new HashMap[String, String]()
            dashboardMap.put("total_order_count", totalOrderCount.toString)
            dashboardMap.put("total_sales_amount", formatDouble(totalSalesAmount))
            dashboardMap.put("total_avg_cost", formatDouble(totalAvgCost))
            dashboardMap.put("latest_batch_id", latestBatchId)
            dashboardMap.put("latest_batch_order_count", latestBatchOrderCount.toString)
            dashboardMap.put("latest_batch_sales_amount", formatDouble(latestBatchSalesAmount))
            dashboardMap.put("latest_batch_avg_cost", formatDouble(latestBatchAvgCost))
            dashboardMap.put("update_time", currentTime())

            jedis.hset("mock_order:dashboard", dashboardMap)

            println("Redis 写入成功：mock_order:dashboard")
            println("======================================")

          } finally {
            jedis.close()
          }
        }
      }
      .start()

    query.awaitTermination()
  }

  private def currentTime(): String = {
    LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
  }

  private def formatDouble(value: Double): String = {
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString()
  }
}