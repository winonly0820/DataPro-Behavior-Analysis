//import org.apache.spark.sql.SparkSession
//import java.time.LocalDateTime
//import java.time.format.DateTimeFormatter
//
//object HiveToEsDaily {
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder()
//      .appName("HiveToEsDaily")
//      .master("local[2]")
//      .config("spark.driver.memory", "1g")
//      .config("spark.executor.memory", "1g")
//      .config("spark.sql.shuffle.partitions", "4")
//      .config("spark.default.parallelism", "4")
//      .enableHiveSupport()
//      .getOrCreate()
//
//    val now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
//
//    val df = spark.sql(s"""
//  select
//    addressoj,
//    owner_code,
//    owner_name,
//    run_name,
//    '$now' as run_time,
//    sm_name,
//    terminal_no,
//    timestamp
//  from datapro_primary.mmconsume_billevents
//  order by rand()
//  limit 500
//""")
//
//
//    df.write
//      .format("es")
//      .option("es.nodes", "node1")
//      .option("es.port", "9200")
//      .option("es.resource", "consume_events/_doc")
//      .option("es.batch.size.entries", "200")
//      .option("es.batch.write.retry.count", "3")
//      .option("es.batch.write.retry.wait", "10s")
//      .mode("append")
//      .save()
//
//    spark.stop()
//  }
//}
