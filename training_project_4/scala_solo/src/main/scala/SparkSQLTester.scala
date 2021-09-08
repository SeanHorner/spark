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
    .filter('category_ids.isNotNull && 'local_date.isNotNull)
    .withColumn("year", 'local_date.substr(0,4).cast(IntegerType))
    .withColumn("month", 'local_date.substr(6,2).cast(IntegerType))
    .withColumn("1", 'category_ids.cast("String").contains("1").cast("int"))
    .withColumn("2", 'category_ids.cast("String").contains("2").cast("int"))
    .withColumn("3", 'category_ids.cast("String").contains("3").cast("int"))
    .withColumn("4", 'category_ids.cast("String").contains("4").cast("int"))
    .withColumn("6", 'category_ids.cast("String").contains("6").cast("int"))
    .withColumn("8", 'category_ids.cast("String").contains("8").cast("int"))
    .withColumn("9", 'category_ids.cast("String").contains("9").cast("int"))
    .withColumn("10", 'category_ids.cast("String").contains("10").cast("int"))
    .withColumn("11", 'category_ids.cast("String").contains("11").cast("int"))
    .withColumn("12", 'category_ids.cast("String").contains("12").cast("int"))
    .withColumn("13", 'category_ids.cast("String").contains("13").cast("int"))
    .withColumn("14", 'category_ids.cast("String").contains("14").cast("int"))
    .withColumn("15", 'category_ids.cast("String").contains("15").cast("int"))
    .withColumn("16", 'category_ids.cast("String").contains("16").cast("int"))
    .withColumn("21", 'category_ids.cast("String").contains("21").cast("int"))
    .withColumn("22", 'category_ids.cast("String").contains("22").cast("int"))
    .withColumn("23", 'category_ids.cast("String").contains("23").cast("int"))
    .withColumn("24", 'category_ids.cast("String").contains("24").cast("int"))
    .withColumn("25", 'category_ids.cast("String").contains("25").cast("int"))
    .withColumn("27", 'category_ids.cast("String").contains("27").cast("int"))
    .withColumn("28", 'category_ids.cast("String").contains("28").cast("int"))
    .withColumn("29", 'category_ids.cast("String").contains("29").cast("int"))
    .withColumn("32", 'category_ids.cast("String").contains("32").cast("int"))
    .withColumn("33", 'category_ids.cast("String").contains("33").cast("int"))
    .withColumn("34", 'category_ids.cast("String").contains("34").cast("int"))
    .withColumn("36", 'category_ids.cast("String").contains("36").cast("int"))
    .groupBy('year, 'month)
    .agg(
      sum("1"),   sum("2"),   sum("3"),   sum("4"),
      sum("6"),   sum("8"),   sum("9"),   sum("10"),
      sum("11"),  sum("12"),  sum("13"),  sum("14"),
      sum("15"),  sum("16"),  sum("21"),  sum("22"),
      sum("23"),  sum("24"),  sum("25"),  sum("27"),
      sum("28"),  sum("29"),  sum("32"),  sum("33"),
      sum("34"),  sum("36"))
    .withColumnRenamed("sum(1)", "Career")
    .withColumnRenamed("sum(2)", "Movements")
    .withColumnRenamed("sum(3)", "3")
    .withColumnRenamed("sum(4)", "4")
    .withColumnRenamed("sum(6)", "Education")
    .withColumnRenamed("sum(8)", "8")
    .withColumnRenamed("sum(9)", "9")
    .withColumnRenamed("sum(10)", "10")
    .withColumnRenamed("sum(11)", "11")
    .withColumnRenamed("sum(12)", "LGBTQ")
    .withColumnRenamed("sum(13)", "13")
    .withColumnRenamed("sum(14)", "14")
    .withColumnRenamed("sum(15)", "Hobbies")
    .withColumnRenamed("sum(16)", "16")
    .withColumnRenamed("sum(21)", "21")
    .withColumnRenamed("sum(22)", "22")
    .withColumnRenamed("sum(23)", "23")
    .withColumnRenamed("sum(24)", "24")
    .withColumnRenamed("sum(25)", "25")
    .withColumnRenamed("sum(27)", "27")
    .withColumnRenamed("sum(28)", "28")
    .withColumnRenamed("sum(29)", "29")
    .withColumnRenamed("sum(32)", "Sports")
    .withColumnRenamed("sum(33)", "33")
    .withColumnRenamed("sum(34)", "34")
    .withColumnRenamed("sum(36)", "Writing")
    .orderBy('year, 'month)

  Q5_base_df.show()
}
