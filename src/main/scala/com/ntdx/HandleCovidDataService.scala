package com.ntdx

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.sql.{SaveMode, SparkSession}

object HandleCovidDataService {
  def main(args: Array[String]): Unit = {
    // 选择用Spark SQL来进行汇总统计
    // 0.初始化SparkSession
    val spark = SparkSession.builder().appName("HandleCovidDataService").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    val jdbcURL = "jdbc:mysql://127.0.0.1:3306/analysis?useSSL=false"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "123456")

    // 从JSON文件读取数据转为DataFrame
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val path = "D:\\covidData\\es_" + sdf.format(new Date()) + ".json"
//    val path = "D:\\covidData\\es_2022-05-26.json"
    val sourceDF = spark.read.json(path)
    sourceDF.createOrReplaceTempView("p_covid")
    // sourceDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "p_covid", properties)

    // 1.全国省份现有确诊人数情况：（按照省份分组，并进行现有确诊人数的统计） - 放在地图中
    // 格式：日期，省份，现有确诊人数
    val sqlMap = "SELECT date, province, sum(confirm-dead-heal) as storeConfirm " +
      "FROM p_covid " +
      "WHERE city NOT IN ('待确认') " +
      "GROUP BY date, province " +
      "ORDER BY storeConfirm DESC"
    val mapDF = spark.sql(sqlMap)
    // mapDF.show()
    // 保存数据库
    mapDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "p_map", properties)

    // 2.上海市辖区累计确诊人数按照累计数量排名
    val sqlSH = "SELECT date, city, confirm " +
      "FROM p_covid " +
      "WHERE province = '上海' " +
      "ORDER BY confirm DESC"
    val shDF = spark.sql(sqlSH)
    shDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "p_sh", properties)

    // 3.全国城市现有确诊人数按照累计数量排名top10
    val sqlTop = "SELECT date, city, confirm-dead-heal as storeConfirm " +
      "FROM p_covid " +
      "WHERE city NOT IN ('待确认', '未明确地区') " +
      "ORDER BY storeConfirm DESC " +
      "LIMIT 10"
    val topDF = spark.sql(sqlTop)
    topDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "p_top", properties)

    // 4.全国较4日数据对比（根据日期分组获取数据进行汇总）
    // 日期 全国累计确诊
    val compareSQL = "SELECT date, compareConfirm " +
      "FROM p_covid " +
      "WHERE city IS NULL"
    val compareDF = spark.sql(compareSQL)
    compareDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "p_compare_confirm", properties)

    // 5.全国确诊、死亡、治愈占比例图（按照累计确诊、累计死亡、累计治愈进行统计）
    val totalSQL = "SELECT date, confirm, dead, heal " +
      "FROM p_covid " +
      "WHERE city IS NULL AND country='中国'"
    val totalDF = spark.sql(totalSQL)
    totalDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "p_total", properties)

    // 6.4日新增确诊和新增无症状感染者趋势图
    val fourDaySQL = "SELECT date, compareConfirm, compareNoSymptom " +
      "FROM p_covid " +
      "WHERE city IS NULL AND country='中国' " +
      "ORDER BY date"
    val fourDayDF = spark.sql(fourDaySQL)
    fourDayDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "p_four_day", properties)
  }
}
