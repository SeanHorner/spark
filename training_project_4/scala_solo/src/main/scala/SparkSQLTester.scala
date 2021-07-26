import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object SparkSQLTester extends App {
  val spark: SparkSession = SparkSession.builder()
    .appName("Meetup Trends Analysis Engine")
    .master("local[4]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._


  val baseDf = spark.read.parquet("all_cities_data.parquet")
  val timeAdjDf = AnalysisHelper.citiesTimeAdj.toDF("location", "time_zone", "hour_adjust", "millis_adjust")
  val df = baseDf.join(timeAdjDf, $"localized_location" === $"location", "outer")

  df.show(5)

  /**
    root
     |-- id: string (nullable = true)
     |-- name: string (nullable = true)
     |-- group_name: string (nullable = true)
     |-- urlname: string (nullable = true)
     |-- v_id: long (nullable = true)
     |-- v_name: string (nullable = true)
     |-- local_date: string (nullable = true)    ("YYYY-MM-DD")
     |-- date_month: string (nullable = true)
     |-- local_time: string (nullable = true)
     |-- localized_location: string (nullable = true)
     |-- is_online_event: boolean (nullable = true)
     |-- status: string (nullable = true)
     |-- category_ids: array (nullable = true)
     |    |-- element: long (containsNull = true)
     |-- duration: long (nullable = true)
     |-- time: long (nullable = true)
     |-- created: long (nullable = true)
     |-- yes_rsvp_count: long (nullable = true)
     |-- rsvp_limit: long (nullable = true)
     |-- accepts: string (nullable = true)
     |-- amount: double (nullable = true)
     |-- description: string (nullable = true)
     |-- location: string (nullable = true)
     |-- time_adjustment: integer (nullable = true)
   */

  // Question 12.  How has the average cost of events changed over time?

  def payments_formatter(str: String): List[String] = {
    if (str.isEmpty)
      List[String]()
    else
      str.split(',').toList
  }
  val payments_formatterUDF: UserDefinedFunction = udf[List[String], String](payments_formatter)

  val Q12_base_df = df
    .select('id, 'local_date, 'accepts, 'amount)
    .filter('accepts.isNotNull)
    .withColumn("year", 'local_date.substr(0, 4).cast(IntegerType))

  var results_base = Seq[Row]()
  for (y <- 2003 to 2020) {
    var temp_seq = Seq[Int](y)
    for(option <- List("cash", "paypal", "wepay")) {
      temp_seq = temp_seq :+ Q12_base_df
        .filter('year === y)
        .filter('accepts === option)
        .count()
        .toInt
    }
    results_base = results_base :+ Row.fromSeq(temp_seq)
  }

  val Q12_results_schema = StructType(
    List(
      StructField("year", IntegerType, nullable = false),
      StructField("Cash", IntegerType, nullable = false),
      StructField("PayPal", IntegerType, nullable = false),
      StructField("WePay", IntegerType, nullable = false)
    )
  )

  val Q12_results_rdd = spark.sparkContext.parallelize(results_base)
  val Q12_results_df = spark.createDataFrame(Q12_results_rdd, Q12_results_schema).orderBy('year)

  Q12_results_df.show(20)

  println("\tWriting data to temp output file...")
  Q12_results_df
    .write
    .format("csv")
    .option("sep", '\t')
    .save("Q12_results")

//  outputCombiner("Q12_results", "output/question_12/results.tsv")

}
