import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import java.io.File

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
