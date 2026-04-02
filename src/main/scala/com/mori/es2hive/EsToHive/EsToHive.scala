package com.mori.es2hive

import org.apache.spark.sql.{DataFrame, SparkSession}

object EsToHive {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("EsToHive")
      .master("local[*]") // 集群环境可改为 yarn
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val esNodes = "192.168.86.101"
    val esPort  = "9200"

    val idxUserMsg       = "idx_mediamatch_usermsg"
    val idxUserEvent     = "idx_mediamatch_userevent"
    val idxBill          = "idx_mmconsume_billevents"
    val idxOrder         = "idx_order_index_test"
    val idxMedia         = "idx_media_index_test"

    def readES(index: String): DataFrame = {
      spark.read
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", esNodes)
        .option("es.port", esPort)
        .option("es.nodes.wan.only", "true")
        .load(index)
    }

    val userMsgDF   = readES(idxUserMsg)
    val userEventDF = readES(idxUserEvent)
    val billDF      = readES(idxBill)
    val orderDF     = readES(idxOrder)
    val mediaDF     = readES(idxMedia)

    println(s"用户基本信息表（ES）数据量: ${userMsgDF.count()}")
    println(s"用户状态变更表（ES）数据量: ${userEventDF.count()}")
    println(s"账单信息表（ES）数据量: ${billDF.count()}")
    println(s"订单信息表（ES）数据量: ${orderDF.count()}")
    println(s"收视行为表（ES）数据量: ${mediaDF.count()}")

    userMsgDF.write.mode("overwrite").saveAsTable("datapro_primary.mediamatch_usermsg")
    userEventDF.write.mode("overwrite").saveAsTable("datapro_primary.mediamatch_userevent")
    billDF.write.mode("overwrite").saveAsTable("datapro_primary.mmconsume_billevents")
    orderDF.write.mode("overwrite").saveAsTable("datapro_primary.order_index_v3")
    mediaDF.write.mode("overwrite").saveAsTable("datapro_primary.media_index_3m")

    println(" ES → Hive 五张表数据导入完成！")

    spark.stop()
  }
}
