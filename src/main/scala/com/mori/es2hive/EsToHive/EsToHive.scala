//package com.mori.es2hive
//import org.apache.spark.sql.{SparkSession, DataFrame, Column}
//import org.apache.spark.sql.functions._
//
//object EsToHive {
//
//  def main(args: Array[String]): Unit = {
//
//    println("程序启动")
//
//    if (args.length < 2) {
//      System.err.println("参数错误：需要传入 esIndex 和 hiveTable")
//      System.err.println("示例：EsToHive mediamatch_usermsg datapro_primary.mediamatch_usermsg")
//      System.exit(1)
//    }
//
//    val esIndex = args(0)
//    val hiveTable = args(1)
//
//    val spark = SparkSession.builder()
//      .appName(s"EsToHive_$esIndex")
//      .enableHiveSupport()
//      .config("es.nodes", "192.168.86.101")
//      .config("es.port", "9200")
//      .config("es.nodes.wan.only", "true")
//      .config("spark.sql.session.timeZone", "Asia/Shanghai")
//      .getOrCreate()
//
//    spark.sparkContext.setLogLevel("WARN")
//
//    println(s"开始读取 ES 索引：$esIndex")
//
//    val esDf = spark.read
//      .format("org.elasticsearch.spark.sql")
//      .load(esIndex)
//
//    println("ES 读取成功")
//    println("ES 原始 Schema：")
//    esDf.printSchema()
//
//    def fieldOrNull(df: DataFrame, fieldName: String, dataType: String): Column = {
//      if (df.columns.contains(fieldName)) {
//        col(fieldName).cast(dataType).as(fieldName)
//      } else {
//        lit(null).cast(dataType).as(fieldName)
//      }
//    }
//
//    val resultDf = esDf.select(
//      fieldOrNull(esDf, "terminal_no", "string"),
//      fieldOrNull(esDf, "phone_no", "string"),
//      fieldOrNull(esDf, "sm_name", "string"),
//      fieldOrNull(esDf, "run_name", "string"),
//      fieldOrNull(esDf, "sm_code", "string"),
//      fieldOrNull(esDf, "owner_name", "string"),
//      fieldOrNull(esDf, "owner_code", "string"),
//      fieldOrNull(esDf, "run_time", "timestamp"),
//      fieldOrNull(esDf, "addressoj", "string"),
//      fieldOrNull(esDf, "estate_name", "string"),
//      fieldOrNull(esDf, "open_time", "timestamp"),
//      fieldOrNull(esDf, "force", "string")
//    )
//
//    println("转换后的 Schema：")
//    resultDf.printSchema()
//
//    println("预览前 5 条数据：")
//    resultDf.show(5, false)
//
//    println(s"准备写入 Hive 表：$hiveTable")
//    println(s"待写入数据量：${resultDf.count()}")
//
//    resultDf.write
//      .mode("append")
//      .insertInto(hiveTable)
//
//    println("Hive 写入成功")
//
//    println("Hive 表当前数据量：")
//    spark.sql(s"SELECT COUNT(*) AS cnt FROM $hiveTable").show(false)
//
//    spark.stop()
//  }
//}




//package com.mori.es2hive
//
//import org.apache.spark.sql.{SparkSession, DataFrame, Column}
//import org.apache.spark.sql.functions._
//
//object EsToHive {
//
//  def main(args: Array[String]): Unit = {
//
//    println("程序启动")
//
//    if (args.length < 2) {
//      System.err.println("参数错误：需要传入 esIndex 和 hiveTable")
//      System.err.println("示例：EsToHive mediamatch_userevent datapro_primary.mediamatch_userevent")
//      System.exit(1)
//    }
//
//    val esIndex = args(0)
//    val hiveTable = args(1)
//
//    val spark = SparkSession.builder()
//      .appName(s"EsToHive_$esIndex")
//      .enableHiveSupport()
//      .config("es.nodes", "192.168.86.101")
//      .config("es.port", "9200")
//      .config("es.nodes.wan.only", "true")
//      .config("spark.sql.session.timeZone", "Asia/Shanghai")
//      .getOrCreate()
//
//    spark.sparkContext.setLogLevel("WARN")
//
//    println(s"开始读取 ES 索引：$esIndex")
//
//    val esDf = spark.read
//      .format("org.elasticsearch.spark.sql")
//      .load(esIndex)
//
//    println("ES 读取成功")
//    println("ES 原始 Schema：")
//    esDf.printSchema()
//
//    def fieldOrNull(df: DataFrame, fieldName: String, dataType: String): Column = {
//      if (df.columns.contains(fieldName)) {
//        col(fieldName).cast(dataType).as(fieldName)
//      } else {
//        lit(null).cast(dataType).as(fieldName)
//      }
//    }
//
//    /*
//      第二个表：mediamatch_userevent
//
//      Hive 表字段顺序：
//      phone_no string
//      run_name string
//      run_time timestamp
//      owner_name string
//      owner_code string
//      open_time timestamp
//      sm_name string
//    */
//
//    val resultDf = esDf.select(
//      fieldOrNull(esDf, "phone_no", "string"),
//      fieldOrNull(esDf, "run_name", "string"),
//      fieldOrNull(esDf, "run_time", "timestamp"),
//      fieldOrNull(esDf, "owner_name", "string"),
//      fieldOrNull(esDf, "owner_code", "string"),
//      fieldOrNull(esDf, "open_time", "timestamp"),
//      fieldOrNull(esDf, "sm_name", "string")
//    )
//
//    println("转换后的 Schema：")
//    resultDf.printSchema()
//
//    println("预览前 5 条数据：")
//    resultDf.show(5, false)
//
//    println(s"准备写入 Hive 表：$hiveTable")
//    println(s"待写入数据量：${resultDf.count()}")
//
//    resultDf.write
//      .mode("append")
//      .insertInto(hiveTable)
//
//    println("Hive 写入成功")
//
//    println("Hive 表当前数据量：")
//    spark.sql(s"SELECT COUNT(*) AS cnt FROM $hiveTable").show(false)
//
//    spark.stop()
//  }
//}








//package com.mori.es2hive
//
//import org.apache.spark.sql.{SparkSession, DataFrame, Column}
//import org.apache.spark.sql.functions._
//
//object EsToHive {
//
//  def main(args: Array[String]): Unit = {
//
//    println("程序启动")
//
//    if (args.length < 2) {
//      System.err.println("参数错误：需要传入 esIndex 和 hiveTable")
//      System.err.println("示例：EsToHive mmconsume_billevents datapro_primary.mmconsume_billevents")
//      System.exit(1)
//    }
//
//    val esIndex = args(0)
//    val hiveTable = args(1)
//
//    val spark = SparkSession.builder()
//      .appName(s"EsToHive_$esIndex")
//      .enableHiveSupport()
//      .config("es.nodes", "192.168.86.101")
//      .config("es.port", "9200")
//      .config("es.nodes.wan.only", "true")
//      .config("spark.sql.session.timeZone", "Asia/Shanghai")
//      .getOrCreate()
//
//    spark.sparkContext.setLogLevel("WARN")
//
//    println(s"开始读取 ES 索引：$esIndex")
//
//    val esDf = spark.read
//      .format("org.elasticsearch.spark.sql")
//      .load(esIndex)
//
//    println("ES 读取成功")
//    println("ES 原始 Schema：")
//    esDf.printSchema()
//
//    def fieldOrNull(df: DataFrame, fieldName: String, dataType: String): Column = {
//      if (df.columns.contains(fieldName)) {
//        col(fieldName).cast(dataType).as(fieldName)
//      } else {
//        lit(null).cast(dataType).as(fieldName)
//      }
//    }
//
//    /*
//      第三个表：mmconsume_billevents
//
//      Hive 表字段顺序：
//      terminal_no string
//      phone_no string
//      fee_code string
//      year_month timestamp
//      owner_name string
//      owner_code string
//      sm_name string
//      should_pay double
//      favour_fee double
//    */
//
//    val resultDf = esDf.select(
//      fieldOrNull(esDf, "terminal_no", "string"),
//      fieldOrNull(esDf, "phone_no", "string"),
//      fieldOrNull(esDf, "fee_code", "string"),
//      fieldOrNull(esDf, "year_month", "timestamp"),
//      fieldOrNull(esDf, "owner_name", "string"),
//      fieldOrNull(esDf, "owner_code", "string"),
//      fieldOrNull(esDf, "sm_name", "string"),
//      fieldOrNull(esDf, "should_pay", "double"),
//      fieldOrNull(esDf, "favour_fee", "double")
//    )
//
//    println("转换后的 Schema：")
//    resultDf.printSchema()
//
//    println("预览前 5 条数据：")
//    resultDf.show(5, false)
//
//    println(s"准备写入 Hive 表：$hiveTable")
//    println(s"待写入数据量：${resultDf.count()}")
//
//    resultDf.write
//      .mode("append")
//      .insertInto(hiveTable)
//
//    println("Hive 写入成功")
//
//    println("Hive 表当前数据量：")
//    spark.sql(s"SELECT COUNT(*) AS cnt FROM $hiveTable").show(false)
//
//    spark.stop()
//  }
//}




//package com.mori.es2hive
//
//import org.apache.spark.sql.{SparkSession, DataFrame, Column}
//import org.apache.spark.sql.functions._
//
//object EsToHive {
//
//  def main(args: Array[String]): Unit = {
//
//    println("程序启动")
//
//    if (args.length < 2) {
//      System.err.println("参数错误：需要传入 esIndex 和 hiveTable")
//      System.err.println("示例：EsToHive order_index_test datapro_primary.order_index_test")
//      System.exit(1)
//    }
//
//    val esIndex = args(0)
//    val hiveTable = args(1)
//
//    val spark = SparkSession.builder()
//      .appName(s"EsToHive_$esIndex")
//      .enableHiveSupport()
//      .config("es.nodes", "192.168.86.101")
//      .config("es.port", "9200")
//      .config("es.nodes.wan.only", "true")
//      .config("spark.sql.session.timeZone", "Asia/Shanghai")
//      .getOrCreate()
//
//    spark.sparkContext.setLogLevel("WARN")
//
//    println(s"开始读取 ES 索引：$esIndex")
//
//    val esDf = spark.read
//      .format("org.elasticsearch.spark.sql")
//      .load(esIndex)
//
//    println("ES 读取成功")
//    println("ES 原始 Schema：")
//    esDf.printSchema()
//
//    def fieldOrNull(df: DataFrame, fieldName: String, dataType: String): Column = {
//      if (df.columns.contains(fieldName)) {
//        col(fieldName).cast(dataType).as(fieldName)
//      } else {
//        lit(null).cast(dataType).as(fieldName)
//      }
//    }
//
//    val resultDf = esDf.select(
//      fieldOrNull(esDf, "phone_no", "string"),
//      fieldOrNull(esDf, "owner_name", "string"),
//      fieldOrNull(esDf, "optdate", "timestamp"),
//      fieldOrNull(esDf, "prodname", "string"),
//      fieldOrNull(esDf, "sm_name", "string"),
//      fieldOrNull(esDf, "offerid", "string"),
//      fieldOrNull(esDf, "offername", "string"),
//      fieldOrNull(esDf, "business_name", "string"),
//      fieldOrNull(esDf, "owner_code", "string"),
//      fieldOrNull(esDf, "prodprcid", "string"),
//      fieldOrNull(esDf, "prodprcname", "string"),
//      fieldOrNull(esDf, "effdate", "timestamp"),
//      fieldOrNull(esDf, "expdate", "timestamp"),
//      fieldOrNull(esDf, "orderdate", "timestamp"),
//      fieldOrNull(esDf, "cost", "double"),
//      fieldOrNull(esDf, "mode_time", "string"),
//      fieldOrNull(esDf, "prodstatus", "string"),
//      fieldOrNull(esDf, "run_name", "string"),
//      fieldOrNull(esDf, "orderno", "string"),
//      fieldOrNull(esDf, "offertype", "string")
//    )
//
//    println("转换后的 Schema：")
//    resultDf.printSchema()
//
//    println("预览前 5 条数据：")
//    resultDf.show(5, false)
//
//    println(s"准备写入 Hive 表：$hiveTable")
//    println(s"待写入数据量：${resultDf.count()}")
//
//    resultDf.write
//      .mode("append")
//      .insertInto(hiveTable)
//
//    println("Hive 写入成功")
//
//    println("Hive 表当前数据量：")
//    spark.sql(s"SELECT COUNT(*) AS cnt FROM $hiveTable").show(false)
//
//    spark.stop()
//  }
//}



package com.mori.es2hive

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._

object EsToHive {

  def main(args: Array[String]): Unit = {

    println("程序启动")

    if (args.length < 2) {
      System.err.println("参数错误：需要传入 esIndex 和 hiveTable")
      System.err.println("示例：EsToHive media_index_test datapro_primary.media_index_test")
      System.exit(1)
    }

    val esIndex = args(0)
    val hiveTable = args(1)

    val spark = SparkSession.builder()
      .appName(s"EsToHive_$esIndex")
      .enableHiveSupport()
      .config("es.nodes", "192.168.86.101")
      .config("es.port", "9200")
      .config("es.nodes.wan.only", "true")
      .config("spark.sql.session.timeZone", "Asia/Shanghai")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println(s"开始读取 ES 索引：$esIndex")

    val esDf = spark.read
      .format("org.elasticsearch.spark.sql")
      // 关键：ES date 字段先按字符串读入，避免 ES-Hadoop 直接解析失败
      .option("es.mapping.date.rich", "false")
      // 只读取目标字段，不读取 tags，避免 tags 数组警告
      .option(
        "es.read.field.include",
        "terminal_no,phone_no,duration,station_name,origin_time,end_time,owner_code,owner_name,vod_cat_tags,resolution,audio_lang,region,res_name,res_type,vod_title,category_name,program_title,sm_name,first_show_time"
      )
      .load(esIndex)

    println("ES 读取成功")
    println("ES 原始 Schema：")
    esDf.printSchema()

    def rawString(df: DataFrame, fieldName: String): Column = {
      if (df.columns.contains(fieldName)) {
        trim(col(fieldName).cast("string"))
      } else {
        lit(null).cast("string")
      }
    }

    def cleanString(df: DataFrame, fieldName: String): Column = {
      val c = rawString(df, fieldName)

      when(c.isNull || c === "" || lower(c) === "null", lit(null).cast("string"))
        .otherwise(c)
        .as(fieldName)
    }

    def cleanLong(df: DataFrame, fieldName: String): Column = {
      val c = rawString(df, fieldName)

      when(c.isNull || c === "" || lower(c) === "null", lit(null).cast("long"))
        .otherwise(c.cast("long"))
        .as(fieldName)
    }

    def parseTimestampFrom(rawCol: Column): Column = {
      val c = trim(rawCol.cast("string"))

      /*
        ISO UTC 时间：
        2018-05-08T23:45:00.000Z
        2018-05-08T23:45:00Z

        先将 T 替换成空格，去掉 Z，再按 UTC 转 Asia/Shanghai。
      */
      val isoClean = regexp_replace(
        regexp_replace(c, "T", " "),
        "Z$",
        ""
      )

      val isoTimestamp = from_utc_timestamp(
        coalesce(
          to_timestamp(isoClean, "yyyy-MM-dd HH:mm:ss.SSS"),
          to_timestamp(isoClean, "yyyy-MM-dd HH:mm:ss")
        ),
        "Asia/Shanghai"
      )

      /*
        本地时间：
        2018-05-08 22:30:00
        2018-5-8 22:30:00
        2018-05-08 22:30
        2018-5-8 22:30
        2018-05-08
      */
      val localTimestamp = coalesce(
        to_timestamp(c, "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(c, "yyyy-M-d HH:mm:ss"),
        to_timestamp(c, "yyyy-M-d H:mm:ss"),
        to_timestamp(c, "yyyy-MM-dd HH:mm"),
        to_timestamp(c, "yyyy-M-d HH:mm"),
        to_timestamp(c, "yyyy-M-d H:mm"),
        to_timestamp(c, "yyyy-MM-dd"),
        to_timestamp(c, "yyyy-M-d")
      )

      when(c.isNull || c === "" || lower(c) === "null", lit(null).cast("timestamp"))
        .when(c.rlike("^\\d{4}-\\d{2}-\\d{2}T.*Z$"), isoTimestamp)
        .when(c.rlike("^\\d{4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{2}:\\d{2}$"), localTimestamp)
        .when(c.rlike("^\\d{4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{2}$"), localTimestamp)
        .when(c.rlike("^\\d{4}-\\d{1,2}-\\d{1,2}$"), localTimestamp)
        .otherwise(lit(null).cast("timestamp"))
    }

    def hasValue(c: Column): Column = {
      c.isNotNull && trim(c) =!= "" && lower(trim(c)) =!= "null"
    }

    val originRaw = rawString(esDf, "origin_time").as("_origin_time_raw")
    val endRaw = rawString(esDf, "end_time").as("_end_time_raw")
    val firstShowRaw = rawString(esDf, "first_show_time").as("_first_show_time_raw")

    val tmpDf = esDf.select(
      cleanString(esDf, "terminal_no"),
      cleanString(esDf, "phone_no"),
      cleanLong(esDf, "duration"),
      cleanString(esDf, "station_name"),

      originRaw,
      parseTimestampFrom(rawString(esDf, "origin_time")).as("origin_time"),

      endRaw,
      parseTimestampFrom(rawString(esDf, "end_time")).as("end_time"),

      cleanString(esDf, "owner_code"),
      cleanString(esDf, "owner_name"),
      cleanString(esDf, "vod_cat_tags"),
      cleanString(esDf, "resolution"),
      cleanString(esDf, "audio_lang"),
      cleanString(esDf, "region"),
      cleanString(esDf, "res_name"),
      cleanString(esDf, "res_type"),
      cleanString(esDf, "vod_title"),
      cleanString(esDf, "category_name"),
      cleanString(esDf, "program_title"),
      cleanString(esDf, "sm_name"),

      firstShowRaw,
      parseTimestampFrom(rawString(esDf, "first_show_time")).as("first_show_time")
    )

    /*
      判断时间转换失败：
      只要原始字段有值，但是转换后为 NULL，就认为该行是脏数据，直接舍弃。

      注意：
      如果 first_show_time 原本就是 NULL 或缺失，不算失败。
    */
    val badTimeCondition =
      (hasValue(col("_origin_time_raw")) && col("origin_time").isNull) ||
        (hasValue(col("_end_time_raw")) && col("end_time").isNull) ||
        (hasValue(col("_first_show_time_raw")) && col("first_show_time").isNull)

    val badDf = tmpDf.filter(badTimeCondition)

    println("时间字段转换失败的数据预览，最多显示 20 条：")
    badDf.select(
      "terminal_no",
      "phone_no",
      "_origin_time_raw",
      "origin_time",
      "_end_time_raw",
      "end_time",
      "_first_show_time_raw",
      "first_show_time"
    ).show(20, false)

    val badCount = badDf.count()
    println(s"时间字段无法转换、将被舍弃的行数：$badCount")

    val resultDf = tmpDf
      .filter(!badTimeCondition)
      .select(
        "terminal_no",
        "phone_no",
        "duration",
        "station_name",
        "origin_time",
        "end_time",
        "owner_code",
        "owner_name",
        "vod_cat_tags",
        "resolution",
        "audio_lang",
        "region",
        "res_name",
        "res_type",
        "vod_title",
        "category_name",
        "program_title",
        "sm_name",
        "first_show_time"
      )

    println("转换后的 Schema：")
    resultDf.printSchema()

    println("预览前 20 条有效数据：")
    resultDf.show(20, false)

    println("时间字段检查：")
    resultDf.select(
      "terminal_no",
      "phone_no",
      "origin_time",
      "end_time",
      "first_show_time"
    ).show(20, false)

    println(s"准备写入 Hive 表：$hiveTable")
    val goodCount = resultDf.count()
    println(s"待写入有效数据量：$goodCount")
    println(s"原始读取数据量约为：${goodCount + badCount}")
    println(s"舍弃脏数据量：$badCount")

    resultDf.write
      .mode("append")
      .insertInto(hiveTable)

    println("Hive 写入成功")

    println("Hive 表当前数据量：")
    spark.sql(s"SELECT COUNT(*) AS cnt FROM $hiveTable").show(false)

    spark.stop()
  }
}