package LiamHood

import java.io.{BufferedWriter, File, FileWriter}

import Shared.description_scrubber
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.StringType

import scala.collection.mutable.ListBuffer
import scala.io.{BufferedSource, Source}

object LiamRunner {

  def mainRunner():Unit = {

    val spark = SparkSession.builder()
      .appName("Meetup Test")
      .master("local[4]")
      .config("spark.driver.memory", "10g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

//    val someevents_group = group_event_to_df(spark, "all_cities_array.json")
//    saveDfToParquet(someevents_group, "all_cities")
      val df = spark.read.parquet("input/all_cities.parquet")


    val analysis14 = Analysis.online_event_count_trend(spark, df)
    saveDfToCsv(analysis14, "output/question14/q14_results.tsv")
    Plots.q14_line_plots(analysis14, "Trend in Online vs In-person Events", "output/question14/online_line")

    val analysis11a = Analysis.fee(spark, df)
    saveDfToCsv(analysis11a, "output/question11/q11_results_a.tsv")
    Plots.q11a_line_plot(analysis11a, "Trend in Fee Type", "output/question11/fee_type_line")

    val analysis11b = Analysis.fee_amount(spark, df)
    saveDfToCsv(analysis11b, "output/question11/q11_results_b.tsv")
    Plots.q11b_line_plot(analysis11b, "Trend in Fee Amount", "output/question11/fee_amount_line")

    val analysis7 = Analysis.topic_trend(spark, df)
    saveDfToCsv(analysis7, "output/question7/q7_results.tsv")
    Plots.q7_line_plots(analysis7, "Trend in Topics", "output/question7/topic_line")

    val analysis7ny = Analysis.topic_trend_ny(spark, df)
    saveDfToCsv(analysis7ny, "output/question7/q7_results_ny.tsv")
    Plots.q7_ny_line_plots(analysis7ny, "Trend in Topics in New York", "output/question7/topic_ny_line")

    val analysis7wa = Analysis.topic_trend_wa(spark, df)
    saveDfToCsv(analysis7wa, "output/question7/q7_results_wa.tsv")
    Plots.q7_wa_line_plots(analysis7wa, "Trend in Topics in Seattle", "output/question7/topic_wa_line")

  }

  def group_url_from_upcoming(spark: SparkSession, jsonpath: String): List[String] = {
    import spark.implicits._
    val origDF = spark.read.option("multiline", "true")
      .json(s"$jsonpath")
      .select(explode($"events") as "event")
    val groupurl = origDF.select($"event.group.urlname").coalesce(1).collect().map(row => row(0).toString).toList
    groupurl
  }

  def group_url_from_groups(spark: SparkSession, jsonpath: String): List[String] = {
    import spark.implicits._
    val origDF = spark.read.option("multiline", "true")
      .json(s"$jsonpath")
    val groupurl = origDF.select($"urlname").coalesce(1).collect().map(row => row(0).toString).toList
    groupurl
  }

  def objects_to_array(input: String, output: String): Unit = {
    var openedFile: BufferedSource = Source.fromFile(input)
    val file = new File(output)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("[")
    for (line <- Source.fromFile(input).getLines) {
      bw.append(line)
      bw.append(", ")
    }
    bw.append("{}]")
    bw.close()
  }

  def group_event_to_df(spark: SparkSession, jsonpath: String): DataFrame = {
    import spark.implicits._
    import org.apache.spark.sql.functions.udf

    val scrub = udf[String, String](description_scrubber.description_scrubber)
    val month = udf[String, String](date_to_month)
    val cat_ids = udf[String, Array[BigInt]](cat_ids_to_string)

    val origDF = spark.read.option("multiline", "true")
      .json(s"$jsonpath")
//      .distinct()
//    origDF.printSchema()
    origDF.select($"id", $"name", $"group.name" as "group_name", $"group.urlname", $"venue.id" as "v_id",
      $"venue.name" as "v_name", $"local_date",
      month($"local_date".cast(StringType)) as "date_month",
      $"local_time",
      $"group.localized_location", $"is_online_event", $"status", $"group.meta_category.category_ids",
//      cat_ids($"group.meta_category.category_ids") as "cat_ids",
      $"duration", $"time", $"created", $"yes_rsvp_count", $"rsvp_limit", $"fee.accepts", $"fee.amount",
      scrub($"description") as "description")
  }

  def saveDfToCsv(df: DataFrame, tsvOutput: String, sep: String = "\t"): Unit = {
    val tmpParquetDir = "Posts.tmp.parquet"

    df
      .coalesce(1)
      .write.
          format("com.databricks.spark.csv").
          option("header", true).
          option("delimiter", sep).
          save(tmpParquetDir)

    val dir = new File(tmpParquetDir)
    val newFileRgex = ".*part-00000.*.csv"
    val tmpTsvFile = dir
      .listFiles
      .filter(_.toString.matches(newFileRgex))(0)
      .toString
    (new File(tmpTsvFile)).renameTo(new File(tsvOutput))

    dir.listFiles.foreach( f => f.delete )
    dir.delete
  }

  def saveDfToParquet(df: DataFrame, output: String): Unit = {
    df.write.parquet((output + ".parquet"))
  }

  def getTextContent(filename: String): Option[String] = {
    var openedFile : BufferedSource = null
    try{
      openedFile = Source.fromFile(filename)
      Some(openedFile.getLines().mkString("\n"))
    } finally{
      if (openedFile != null) openedFile.close()
    }
  }

  def date_to_month(date: String): String = {
    var short_date = date + " "
    if (short_date.matches("\\d+-\\d+-\\d+ ")) {
      short_date = date.replaceFirst("(\\d+-\\d+)-\\d+", "$1")
    }
    short_date
  }

  def cat_ids_to_string(input_ids: Array[BigInt]): String = {
    var cat_ids = ""
    for (id <- input_ids){
      cat_ids += id + ", "
    }
    cat_ids.dropRight(2)
    cat_ids
  }
}
