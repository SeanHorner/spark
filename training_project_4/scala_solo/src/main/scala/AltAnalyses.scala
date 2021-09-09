import SparkSQLTester.df
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import java.io.File
import java.util.Date

object AltAnalyses extends App {

  // Here initializing the SparkContext for the
  val spark: SparkSession = SparkSession.builder()
    .appName("Meetup Trends Analysis Engine")
    .master("local[4]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  // ***********************************************************************************************************
  // *
  // *                            Reading in and Formatting the DataFrame
  // *
  // ***********************************************************************************************************

  // Initial data set read in from .tsv file
  val baseDf: DataFrame = spark.read.parquet("all_cities.parquet")

  // Additional timezone and time offset data
  val timeAdjDf: DataFrame = AnalysisHelper.citiesTimeAdj
    .toDF("location", "time_zone", "hour_adjustment", "millis_adjustment")

  // Joined into a single DataFrame
  val df: DataFrame =
    baseDf.join(timeAdjDf, $"localized_location" === $"location", "outer")

  // Cleaning up excess DataFrames
  baseDf.unpersist()
  timeAdjDf.unpersist()


  def alt_analysis1(df: DataFrame): Unit = {
    println("Analysis 1 initialized...")

    println("Building the base DataFrame...")
    val Q1_base_df =
      df.select('id, 'local_date)
        .withColumn("year", 'local_date.substr(0,4).cast(IntegerType))
        .withColumn("month", 'local_date.substr(6,2).cast(IntegerType))

    println("Looping through years and months for count...")
    var results_base = Seq[Row]()
    for (y <- 2003 to 2020) {
      println(s"\tFor year $y...")
      var temp_seq = Seq[Int](y)
      for(m <- 1 to 12) {
        println(s"\t..month $m...")
        temp_seq = temp_seq :+ Q1_base_df
          .filter('year === y)
          .filter('month === m)
          .count()
          .toInt
      }
      results_base = results_base :+ Row.fromSeq(temp_seq)
    }

    val Q1_results_schema = StructType(
      List(
        StructField("year", IntegerType, nullable = false),
        StructField("Jan", IntegerType, nullable = false),
        StructField("Feb", IntegerType, nullable = false),
        StructField("Mar", IntegerType, nullable = false),
        StructField("Apr", IntegerType, nullable = false),
        StructField("May", IntegerType, nullable = false),
        StructField("Jun", IntegerType, nullable = false),
        StructField("Jul", IntegerType, nullable = false),
        StructField("Aug", IntegerType, nullable = false),
        StructField("Sep", IntegerType, nullable = false),
        StructField("Oct", IntegerType, nullable = false),
        StructField("Nov", IntegerType, nullable = false),
        StructField("Dec", IntegerType, nullable = false)
      )
    )

    val Q1_results_rdd = spark.sparkContext.parallelize(results_base)
    val Q1_results_df = spark.createDataFrame(Q1_results_rdd, Q1_results_schema).orderBy('year)

    Q1_results_df.show()

    println("Writing data to temp output file...")
    Q1_results_df.write.csv("output/temp/Q1_results")
    outputCombiner("output/temp/Q1_results", "output/question_01", "results")

    println("Cleaning up DataFrames...")
    Q1_results_df.unpersist()

    println("Beginning visualization creation...")
    /**
     * Data visualization logic goes here
     */

    println("*** Analysis finished. ***\n\n")
  }

  def alt_analysis12(df: DataFrame): Unit = {
    def prepTime(year: Int): Long = {
      val startMillis: Long = new Date(year).getTime
      val endMillis: Long = new Date(year + 1).getTime

      val step1_DF =
        df
          .filter(df("created").isNotNull && df("time").isNotNull)
          .filter(df("time") > startMillis && df("time") <= endMillis)
          .withColumn("prep_period_mins", (df("time") - df("created")) / 600000)

      val step2_DF =
        step1_DF
          .filter(df("prep_period_mins") >= 0)
          .groupBy(df("prep_period_mins"))
          .count()
          .withColumn("prep_time_product", 'prep_period_mins * 'count)

      val total_prep_time: Long =
        step2_DF.agg(sum("prep_time_product")).first.getLong(0)

      val total_events: Long =
        step2_DF.agg(sum("count")).first.getLong(0)

      step1_DF.unpersist()
      step2_DF.unpersist()

      total_prep_time / total_events
    }

    println("Beginning yearly average prep time calculations...")

    var Q12List = List[Long]()
    for(y <- 2003 to 2020) {
      println(s"\tCalculating average prep time for $y...")
      Q12List = Q12List :+ prepTime(y)
    }

    println("Compiling and creating results DataFrame...")

    val Q12_df = Seq(
      (2003, Q12List(0)),  (2004, Q12List(1)),  (2005, Q12List(2)),
      (2006, Q12List(3)),  (2007, Q12List(4)),  (2008, Q12List(5)),
      (2009, Q12List(6)),  (2010, Q12List(7)),  (2011, Q12List(8)),
      (2012, Q12List(9)),  (2013, Q12List(10)), (2014, Q12List(11)),
      (2015, Q12List(12)), (2016, Q12List(13)), (2017, Q12List(14)),
      (2018, Q12List(15)), (2019, Q12List(16)), (2020, Q12List(17)),
    ).toDF("year", "avg_prep_time")

    println("Showing sample of results...")
    Q12_df.show(10)

    println("Saving analysis results to temp file...")
    Q12_df.write.option("header", "true").csv("output/temp/Q12_results")
    outputCombiner("output/temp/Q12_results", "output/question_12", "full_set")

    println("Cleaning up results DataFrame...")
    Q12_df.unpersist()

    println("Beginning visualization creation...")
    /**
     * Data visualization logic goes here
     */

    println("*** Analysis finished. ***\n\n")
  }

  // ***********************************************************************************************************
  // *
  // *                                  Output Combiner and Directory Cleaner
  // *
  // ***********************************************************************************************************

  def outputCombiner(inPath: String, outPath: String, title: String): Unit = {
    // Ensuring the output path is a correctly named filetype, ending in either .tsv or .csv.
    println(s"Moving temp data files to: $outPath/$title.csv")

    // Opening the home directory
    val directory = new File(inPath)

    // Created a Regex expression to look for .csv parts
    val newFileRegex = ".*part-00000.*.csv"

    // Creating one long string containing all of the partial .csv files as one long string.
    val tmpTsvFile = directory
      .listFiles()
      .filter(_.toString.matches(newFileRegex))(0)
      .toString

    // Creating a file from the combined part files, using the File(s: String) constructor,
    // and then renaming that file to the desired output.
    new File(outPath).mkdirs()
    new File(tmpTsvFile).renameTo(new File(s"$outPath/$title.csv"))

    // Directory clean up, deleting each file in the directory then the directory itself.
    directory.listFiles.foreach(f => f.delete())
    directory.delete()
  }
}
