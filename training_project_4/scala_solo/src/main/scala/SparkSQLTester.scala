import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{collect_set, udf}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object SparkSQLTester extends App {
  val spark: SparkSession = SparkSession.builder()
    .appName("Meetup Trends Analysis Engine")
    .master("local[4]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  import org.apache.spark.sql.functions._


  val baseDf = spark.read.parquet("all_cities.parquet")
  val timeAdjDf = AnalysisHelper.citiesTimeAdj.toDF("location", "time_zone", "hour_adjust", "millis_adjust")
  val df = baseDf.join(timeAdjDf, $"localized_location" === $"location", "outer")

//    root
//     |-- id: string (nullable = true)
//     |-- name: string (nullable = true)
//     |-- group_name: string (nullable = true)
//     |-- urlname: string (nullable = true)
//     |-- v_id: long (nullable = true)
//     |-- v_name: string (nullable = true)
//     |-- local_date: string (nullable = true)    ("YYYY-MM-DD")
//     |-- date_month: string (nullable = true)
//     |-- local_time: string (nullable = true)
//     |-- localized_location: string (nullable = true)
//     |-- is_online_event: boolean (nullable = true)
//     |-- status: string (nullable = true)
//     |-- category_ids: array (nullable = true)
//     |    |-- element: long (containsNull = true)
//     |-- duration: long (nullable = true)
//     |-- time: long (nullable = true)
//     |-- created: long (nullable = true)
//     |-- yes_rsvp_count: long (nullable = true)
//     |-- rsvp_limit: long (nullable = true)
//     |-- accepts: string (nullable = true)
//     |-- amount: double (nullable = true)
//     |-- description: string (nullable = true)
//     |-- location: string (nullable = true)
//     |-- time_adjustment: integer (nullable = true)

//  df.show(5)
//  +-------------+--------------------+--------------------+--------------------+-------+--------------------+----------+----------+----------+------------------+---------------+------+------------+--------+-------------+-------------+--------------+----------+-------+------+--------------------+----------------+---------+-----------+-------------+
//  |           id|                name|          group_name|             urlname|   v_id|              v_name|local_date|date_month|local_time|localized_location|is_online_event|status|category_ids|duration|         time|      created|yes_rsvp_count|rsvp_limit|accepts|amount|         description|        location|time_zone|hour_adjust|millis_adjust|
//  +-------------+--------------------+--------------------+--------------------+-------+--------------------+----------+----------+----------+------------------+---------------+------+------------+--------+-------------+-------------+--------------+----------+-------+------+--------------------+----------------+---------+-----------+-------------+
//  |    239675563|   Hello Angular RVA|         Angular RVA|              NG-RVA|   null|                null|2017-06-07|   2017-06|     18:00|  Chesterfield, VA|          false|  past|        [34]| 9000000|1496872800000|1493759483000|            11|      null|   null|  null|kickoff  event  s...|Chesterfield, VA|      EST|         -5|    -18000000|
//  | qbchhdyrpbhb|  Tuesday Open House|Gainesville Hacke...|gainesville-hacke...|8826022|Gainesville Hacke...|2013-11-05|   2013-11|     19:00|   Gainesville, FL|          false|  past|        [34]|    null|1383696000000|1345560395000|             4|      null|   null|  null|tuesday  nights  ...| Gainesville, FL|      EST|         -5|    -18000000|
//  | cqrzglytqbcb|  Tuesday Open House|Gainesville Hacke...|gainesville-hacke...|8826022|Gainesville Hacke...|2015-12-01|   2015-12|     19:00|   Gainesville, FL|          false|  past|        [34]|17100000|1449014400000|1431440034000|             1|      null|   null|  null|tuesday  nights  ...| Gainesville, FL|      EST|         -5|    -18000000|
//  |qmnfslybclbgb|       Board Meeting|Gainesville Hacke...|gainesville-hacke...|8826022|Gainesville Hacke...|2020-08-04|   2020-08|     18:00|   Gainesville, FL|          false|  past|        [34]|    null|1596578400000|1459808812000|             1|      null|   null|  null|regular  board  m...| Gainesville, FL|      EST|         -5|    -18000000|
//  |    115124832|Understanding Wor...|Gainesville WordP...|    WordPress-Meetup|5928012|Santa Fe College ...|2013-05-16|   2013-05|     18:00|   Gainesville, FL|          false|  past|        [34]|    null|1368741600000|1366363670000|            21|      null|   null|  null|customize  wordpr...| Gainesville, FL|      EST|         -5|    -18000000|
//  +-------------+--------------------+--------------------+--------------------+-------+--------------------+----------+----------+----------+------------------+---------------+------+------------+--------+-------------+-------------+--------------+----------+-------+------+--------------------+----------------+---------+-----------+-------------+

  // Q13.  Biggest tech meetup group

  // List(1, 2, 3, 4, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16, 21, 22, 23, 24, 25, 27, 28, 29, 32, 33, 34, 36)

  val Q5_base_df = df
    .select('local_date, 'category_ids)
    .filter('category_ids.isNotNull)
    .withColumn("year", 'local_date.substr(0,4).cast(IntegerType))
    .withColumn("month", 'local_date.substr(6,2).cast(IntegerType))
    .withColumn("1", 'category_ids.contains(1))
    .withColumn("2", 'category_ids.contains(2))
    .withColumn("3", 'category_ids.contains(3))
    .withColumn("4", 'category_ids.contains(4))
    .withColumn("6", 'category_ids.contains(6))
    .withColumn("8", 'category_ids.contains(8))
    .withColumn("9", 'category_ids.contains(9))
    .withColumn("10", 'category_ids.contains(10))
    .withColumn("11", 'category_ids.contains(11))
    .withColumn("12", 'category_ids.contains(12))
    .withColumn("13", 'category_ids.contains(13))
    .withColumn("14", 'category_ids.contains(14))
    .withColumn("15", 'category_ids.contains(15))
    .withColumn("16", 'category_ids.contains(16))
    .withColumn("21", 'category_ids.contains(21))
    .withColumn("22", 'category_ids.contains(22))
    .withColumn("23", 'category_ids.contains(23))
    .withColumn("24", 'category_ids.contains(24))
    .withColumn("25", 'category_ids.contains(25))
    .withColumn("27", 'category_ids.contains(27))
    .withColumn("28", 'category_ids.contains(28))
    .withColumn("29", 'category_ids.contains(29))
    .withColumn("32", 'category_ids.contains(32))
    .withColumn("33", 'category_ids.contains(33))
    .withColumn("34", 'category_ids.contains(34))
    .withColumn("36", 'category_ids.contains(36))
    .groupBy('year, 'month)
    .agg(
      sum("1"),
      sum("2"),
      sum("3"),
      sum("4"),
      sum("6"),
      sum("8"),
      sum("9"),
      sum("10"),
      sum("11"),
      sum("12"),
      sum("13"),
      sum("14"),
      sum("15"),
      sum("16"),
      sum("21"),
      sum("22"),
      sum("23"),
      sum("24"),
      sum("25"),
      sum("27"),
      sum("28"),
      sum("29"),
      sum("32"),
      sum("33"),
      sum("34"),
      sum("36")
    )

  Q5_base_df.show()
}
