package LiamHood

import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Analysis {

  def total_events(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    val new_data = data
      .select($"date_month", $"status")
      .where($"date_month" < "2021-01")
      .where($"status" === "past")
      .groupBy($"date_month")
      .count()
      .sort("date_month")

    new_data
  }

  def online_event_count_trend(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    val new_data_online = data
      .select($"date_month" as "date_month_temp", $"is_online_event", $"status")
      .where($"status" === "past")
      .where($"is_online_event" === true)
      .groupBy($"date_month_temp")
      .count()
      .withColumnRenamed("count", "online_count")

    val new_data_off = data
      .select($"date_month", $"is_online_event", $"status")
      .where($"status" === "past")
      .where($"is_online_event" === false)
      .groupBy($"date_month")
      .count()
      .withColumnRenamed("count", "inperson_count")
    val new_data = new_data_online

      .join(new_data_off,  $"date_month" === $"date_month_temp", joinType = "full_outer")
      .where($"date_month" < "2021-02")
      .na.fill(0, Seq("inperson_count", "online_count"))
      .select($"date_month", $"online_count", $"inperson_count")
      .sort("date_month")

    new_data
  }

  def event_per_group(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._
    val new_data = data
      .select($"group_name")
      .groupBy($"group_name")
      .count()
      .withColumnRenamed("count", "total_events")
      .sort(desc("total_events"))

    new_data
  }

  def fee(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._
    val ness_data = data
      .select($"date_month", $"accepts", $"status")
      .na.fill("free", Array("accepts"))
      .where($"status" === "past")
      .where($"date_month" < "2021-02")
      .drop($"status")

    val free_data = ness_data
      .where($"accepts" === "free")
      .groupBy($"date_month")
      .agg(count("accepts") as "free")
    val cash_data = ness_data
      .where($"accepts" === "cash")
      .groupBy($"date_month" as "cash_date")
      .agg(count("accepts") as "cash")
    val paypal_data = ness_data
      .where($"accepts" === "paypal")
      .groupBy($"date_month" as "paypal_date")
      .agg(count("accepts") as "paypal")
    val wepay_data = ness_data
      .where($"accepts" === "wepay")
      .groupBy($"date_month" as "wepay_date")
      .agg(count("accepts") as "wepay")

    val type_data = free_data
      .join(cash_data, $"date_month" === $"cash_date", "full_outer")
      .join(paypal_data, $"date_month" === $"paypal_date", "full_outer")
      .join(wepay_data, $"date_month" === $"wepay_date", "full_outer")
      .na.fill(0, Seq("free", "cash", "paypal", "wepay"))

    type_data
      .select($"date_month", $"free", $"cash", $"paypal", $"wepay")
      .sort(asc("date_month"))
  }

  def fee_amount(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._
    val ness_data = data
      .select($"date_month", $"accepts", $"amount", $"status")
      .na.fill(0, Array("amount"))
      .where($"status" === "past")
      .where($"date_month" < "2021-02")
      .where($"amount" > 0)
      .drop($"status")

    val max_fee_val = ness_data
      .select(max($"amount"))
      .collect()
      .toList
      .map(_(0).toString().toDouble)

    val max_fee = max_fee_val(0)

    val cash_data = ness_data
      .where($"accepts" === "cash")
      .groupBy($"date_month" as "cash_date")
      .agg(avg($"amount".cast(DoubleType)) as "cash_fee")

    val paypal_data = ness_data
      .where($"accepts" === "paypal")
      .groupBy($"date_month" as "paypal_date")
      .agg(avg($"amount".cast(DoubleType)) as "paypal_fee")

    val wepay_data = ness_data
      .where($"accepts" === "wepay")
      .where($"amount" < max_fee)
      .groupBy($"date_month" as "wepay_date")
      .agg(avg($"amount".cast(DoubleType)) as "wepay_fee")


    val tot_data = ness_data
      .where($"amount" < max_fee)
      .groupBy($"date_month")
      .agg(avg($"amount".cast(DoubleType)) as "average_fee")
      .join(cash_data, $"date_month" === $"cash_date", "full_outer")
      .join(paypal_data, $"date_month" === $"paypal_date", "full_outer")
      .join(wepay_data, $"date_month" === $"wepay_date", "full_outer")
      .na.fill(0, Seq("cash_fee", "paypal_fee", "wepay_fee", "average_fee"))
      .select($"date_month", $"cash_fee", $"paypal_fee", $"wepay_fee", $"average_fee")

    tot_data.sort(asc("date_month"))
  }

  def topic_trend(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._
    val topic_list = data
      .select(explode($"category_ids") as "topic")
      .groupBy("topic")
      .count()
      .sort(desc("count"))
      .head(8)
      .map(_(0).toString())
      .toList

    var new_data = data
      .select($"date_month")
      .where($"date_month" < "2021-02")
      .groupBy($"date_month")
      .count()
      .distinct()
    for (topic <- topic_list){
      new_data = new_data.join(
        data
          .select($"date_month" as "date_month_temp", explode($"category_ids") as "topic")
          .where($"date_month_temp" < "2021-02")
          .where($"topic" === topic)
          .groupBy($"date_month_temp", $"topic")
          .agg(count("topic") as s"${topic}_count"),
        $"date_month" === $"date_month_temp",
        "full_outer"
      )
        .na.fill(0, Seq(s"${topic}_count"))
        .withColumn(s"${topic}_prop", col(s"${topic}_count")/col("count")*100)
        .drop(s"${topic}_count")
        .drop("date_month_temp")
        .drop("topic")
    }

    new_data
      .sort(asc("date_month"))
  }

  def topic_trend_ny(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._
    val topic_list = data
      .select(explode($"category_ids") as "topic")
      .where($"localized_location" === "New York, NY")
      .groupBy("topic")
      .count()
      .sort(desc("count"))
      .head(8)
      .map(_(0).toString())
      .toList

    var new_data = data
      .select($"date_month")
      .where($"localized_location" === "New York, NY")
      .where($"date_month" < "2021-02")
      .groupBy($"date_month")
      .count()
      .distinct()
    for (topic <- topic_list){
      new_data = new_data.join(
        data
          .select($"date_month" as "date_month_temp", explode($"category_ids") as "topic")
          .where($"localized_location" === "New York, NY")
          .where($"date_month_temp" < "2021-02")
          .where($"topic" === topic)
          .groupBy($"date_month_temp", $"topic")
          .agg(count("topic") as s"${topic}_count"),
        $"date_month" === $"date_month_temp",
        "full_outer"
      )
        .na.fill(0, Seq(s"${topic}_count"))
        .withColumn(s"${topic}_prop", col(s"${topic}_count")/col("count")*100)
        .drop(s"${topic}_count")
        .drop("date_month_temp")
        .drop("topic")
    }
    new_data
      .where($"date_month".isNotNull)
      .sort(asc("date_month"))
  }

  def topic_trend_wa(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._
    val topic_list = data
      .select(explode($"category_ids") as "topic")
      .where($"localized_location" === "Seattle, WA")
      .groupBy("topic")
      .count()
      .sort(desc("count"))
      .head(8)
      .map(_(0).toString())
      .toList

    var new_data = data
      .select($"date_month")
      .where($"localized_location" === "Seattle, WA")
      .where($"date_month" < "2021-02")
      .groupBy($"date_month")
      .count()
      .distinct()
    for (topic <- topic_list){
      new_data = new_data.join(
        data
          .select($"date_month" as "date_month_temp", explode($"category_ids") as "topic")
          .where($"localized_location" === "New York, NY")
          .where($"date_month_temp" < "2021-02")
          .where($"topic" === topic)
          .groupBy($"date_month_temp", $"topic")
          .agg(count("topic") as s"${topic}_count"),
        $"date_month" === $"date_month_temp",
        "full_outer"
      )
        .na.fill(0, Seq(s"${topic}_count"))
        .withColumn(s"${topic}_prop", col(s"${topic}_count")/col("count")*100)
        .drop(s"${topic}_count")
        .drop("date_month_temp")
        .drop("topic")
    }
    new_data
      .where($"date_month".isNotNull)
      .sort(asc("date_month"))
  }

  def date_to_month(date: String): String = {
    date.replaceFirst("(\\d+-\\d+)-\\d+", "$1")
  }

}
