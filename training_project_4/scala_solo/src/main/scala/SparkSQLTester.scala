import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction


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

  // Q6.  What is the most popular time when events are created?

  // UDF to calculate in which minute of the day an event was created
  def min_of_day(time: Long): Int = ((time % 8640000) / 60000).toInt
  val min_of_dayUDF: UserDefinedFunction = udf[Int, Long](min_of_day)

  val Q6_byCount =
    df.select('created, 'millis_adjust)
      .filter('created.isNotNull && 'millis_adjust.isNotNull)
      .withColumn("minute_of_day_created", min_of_dayUDF('created + 'millis_adjust))
      .groupBy('minute_of_day_created)
      .count()
      .orderBy('count.desc)

  Q6_byCount.show(10)

  println("Writing the ranked dataset to temp file...")
  Q6_byCount.write.option("header", "true").csv("output/temp/Q6_byCount")
  OutputCombinerTester.outputCombiner("output/temp/Q6_byCount", "output/question_06", "by_count" )

  println("Writing the chronological dataset to temp file...")
  Q6_byCount
    .orderBy('minute_of_day_created)     // Reordering the dataset to chronological order
    .write.option("header", "true").csv("output/temp/Q6_chronological")
  OutputCombinerTester.outputCombiner("output/temp/Q6_chronological", "output/question_06", "by_minute" )
}
