//
//  This analysis engine requires the all_cities.parquet file located at:
//  https://drive.google.com/file/d/1ErurkaXa_LqzxXrN8GPabtGywRZK2Xdu/view?usp=sharing
//
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.defaultTheme
import com.cibo.evilplot.plot._
import org.apache.spark.sql.catalyst.expressions.aggregate.Max

import scala.io.StdIn
import java.io.{File, FileWriter, PrintWriter}
import java.util.Date

object MainRunner extends App {

  printWelcome()
  println()

  // Clearing up any previous output directories
  def recursive_delete(file: File): Unit = {
    if(file.isDirectory) {
      file.listFiles.foreach(recursive_delete)
      file.delete()
    } else {
      file.delete()
    }
  }

  val ae = new AnalysisEngine

  // Setting up an infinite, but exit-able, operating loop
  var proceed = true
  do {
    printMenu()

    val userInput: String = StdIn.readLine()
    val userInputList: List[String] =
      userInput
        .split(' ')                 // for space delimited lists
        .flatMap(_.split(','))      // for comma delimited lists
        .map(_.trim)                // trim all
        .sorted                     // sort the strings for logic and order
        .toList

    // Setting up the list of analyses to be performed.
    var analysesToRun: List[Int] = List()
    if (userInputList.last.equalsIgnoreCase("exit"))
      analysesToRun = List(0)
    else if (userInputList.last.equalsIgnoreCase("all"))
      analysesToRun = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)
    else
      analysesToRun = userInputList.map(_.toInt).sorted

    analysesToRun.foreach {
      case  0 => proceed = false
      case  1 => ae.analysis1()
      case  2 => ae.analysis2()
      case  3 => ae.analysis3()
      case  4 => ae.analysis4()
      case  5 => ae.analysis5()
      case  6 => ae.analysis6()
      case  7 => ae.analysis7()
      case  8 => ae.analysis8()
      case  9 => ae.analysis9()
      case 10 => ae.analysis10()
      case 11 => ae.analysis11()
      case 12 => ae.analysis12()
      case 13 => ae.analysis13()
    }

    // Cleaning up the temp folder
    recursive_delete(new File("output/temp"))

  } while (proceed)

  // ***********************************************************************************************************
  // *
  // *                                        Menu Block Text
  // *
  // ***********************************************************************************************************

  def printWelcome(): Unit = {
    println(
      "*************************************************************************************************************\n"
    + "|                      Welcome to the MeetUp.com Tech Event Data Analysis Tool                              |\n"
    + "|                                                                                                           |\n"
    + "| Originally created in association with Liam Hood, Kyle Pacheco, Michael Splaver, and Quan Vu.             |\n"
    + "| That joint effort can be found at: https://github.com/SeanHorner/spark/tree/main/training_project_4/scala |\n"
    + "|                                                                                                           |\n"
    + "| This rendition of the project is a work entirely of my effort, in an attempt apply new methods and styles |\n"
    + "| learned while continuing to expand my knowledge in the various Spark APIs.                                |\n"
    + "*************************************************************************************************************\n")
  }

  def printMenu(): Unit = {
    print(
      "*************************************************************************************************************\n"
    + "|   1.  How many events were created for each month and year?                                               |\n"
    + "|   2.  How many upcoming events are being hosted online compared to in-person?                             |\n"
    + "|   3.  What is the trend of events about new technologies vs. older ones?                                  |\n"
    + "|   4.  Which cities hosted the most technology-based events? Which venues?                                 |\n"
    + "|   5.  What are some of the most common event topics?                                                      |\n"
    + "|   6.  What is the most popular time when events are created?                                              |\n"
    + "|   7.  Are events with longer durations more popular than shorter durations?                               |\n"
    + "|   8.  Which events have the most RSVPs?                                                                   |\n"
    + "|   9.  How has event capacity changed over the months/years?                                               |\n"
    + "|  10.  What is the preferred payment method for events?                                                    |\n"
    + "|  11.  How has the average cost of events changed over time?                                               |\n"
    + "|  12.  Has there been a change in planning times for events?                                               |\n"
    + "|  13.  What is the largest tech-related group on MeetUp.com?                                               |\n" // <-
    + "|  To run more than one analysis, enter a comma- or space-separated list.                                   |\n"
    + "| All.  Run all of analyses in order.                                                                       |\n"
    + "| Exit. Exit the program.                                                                                   |\n"
    + "*************************************************************************************************************\n"
    + "  Please enter the command or list of analysis numbers:  ")
  }

  // ***********************************************************************************************************
  // *
  // *                                            Analysis Engine
  // *
  // ***********************************************************************************************************

  class AnalysisEngine {

    // Here initializing the SparkContext for the AnalysisEngine to run queries through
    val spark: SparkSession = SparkSession.builder()
      .appName("Meetup Trends Analysis Engine")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // ***********************************************************************************************************
    // *
    // *                  User Defined Functions (UDFs) for DataFrame column manipulation
    // *
    // ***********************************************************************************************************

    // Pair of UDFs to convert the category array to integers and count the number of categories [Analysis 5]
    def category_array_converter(cat_list: List[Long]): List[Int] = cat_list.map(_.toInt)
    val category_array_converterUDF: UserDefinedFunction = udf[List[Int], List[Long]](category_array_converter)

    def category_array_counter(cat_list: List[Int]): Int = cat_list.length
    val category_array_counterUDF: UserDefinedFunction = udf[Int, List[Int]](category_array_counter)

    // UDF to calculate in which minute of the day an event was created [Analysis 6]
    def min_of_day(time: Long): Int = ((time % 8640000) / 60000).toInt

    val min_of_dayUDF: UserDefinedFunction = udf[Int, Long](min_of_day)

    //    // UDF function for converting between milliseconds and minutes returning Int [Analysis 7]
    //    def millisToMinutes(millis: Long): Int = (millis / 600000).toInt
    //
    //    val milliToMinUDF: UserDefinedFunction = udf[Int, Long](millisToMinutes)

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

    // ***********************************************************************************************************
    // *
    // *                                           Analysis Runners
    // *
    // ***********************************************************************************************************

    def analysis1(): Unit = {
      println("Analysis 1 initialized...")

      println("Building the base DataFrame...")
      val Q1_base_df =
        df.select('local_date)
          .filter('local_date.isNotNull)
          .withColumn("year", 'local_date.substr(0,4).cast(IntegerType))
          .withColumn("month", 'local_date.substr(6,2).cast(IntegerType))
          .groupBy('year, 'month)
          .count()
          .orderBy('year, 'month)

      Q1_base_df.show()

      println("Writing data to temp output file...")
      Q1_base_df.write.option("header", "true").csv("output/temp/Q1_results")
      outputCombiner("output/temp/Q1_results", "output/question_01", "results")

      println("Cleaning up DataFrames...")
      Q1_base_df.unpersist()

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis2(): Unit = {
      println("Analysis 2 initialized...")

      val Q2_base_df =
        df.select('local_date, 'is_online_event)
          .filter('local_date.isNotNull)
          .withColumn("year", 'local_date.substr(0,4).cast(IntegerType))
          .withColumn("month", 'local_date.substr(6,2).cast(IntegerType))
          .withColumn("is_online_event", 'is_online_event.cast(IntegerType))
          .groupBy('year, 'month).agg(sum("is_online_event"), count("local_date"))
          .withColumnRenamed("count(local_date)", "count")
          .withColumnRenamed("sum(is_online_event)", "is_online")
          .withColumn("is_offline", 'count - 'is_online)
          .drop("count")
          .orderBy('year, 'month)

      Q2_base_df.show(10)

      println("Saving analysis results to temp file...")
      Q2_base_df.write.option("header", "true").csv("output/temp/Q2_results")
      outputCombiner("output/temp/Q2_results", "output/question_02", "online_vs_offline_events")

      println("Cleaning up DataFrames...")
      Q2_base_df.unpersist()

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis3(): Unit = {
      println("Analysis 3 initialized...")

      // Add tech era as new columns adding up their respective mentions.

//      val tech_list = List(
//        "ada", "android", "clojure", "cobol", "dart", "delphi", "fortran", "ios", "java", "javascript",
//        "kotlin", "labview", "matlab", "pascal", "perl", "php", "powershell", "python", "ruby", "rust",
//        "scala", "sql", "typescript", "visual basic", "wolfram")

      println("Modifying, filtering, and calculating...")
      val Q3_base_df =
        df.select('id, 'local_date, 'description)
          .withColumn("year", 'local_date.substr(0,4).cast(IntegerType))
          .withColumn("month", 'local_date.substr(6,2).cast(IntegerType))
          .withColumn("ada", 'description.contains("ada").cast(IntegerType))
          .withColumn("android", 'description.contains("android").cast(IntegerType))
          .withColumn("clojure", 'description.contains("clojure").cast(IntegerType))
          .withColumn("cobol", 'description.contains("cobol").cast(IntegerType))
          .withColumn("dart", 'description.contains("dart").cast(IntegerType))
          .withColumn("delphi", 'description.contains("delphi").cast(IntegerType))
          .withColumn("fortran", 'description.contains("fortran").cast(IntegerType))
          .withColumn("ios", 'description.contains("ios").cast(IntegerType))
          .withColumn("java", 'description.contains("java").cast(IntegerType))
          .withColumn("javascript", 'description.contains("javascript").cast(IntegerType))
          .withColumn("kotlin", 'description.contains("kotlin").cast(IntegerType))
          .withColumn("labview", 'description.contains("labview").cast(IntegerType))
          .withColumn("matlab", 'description.contains("matlab").cast(IntegerType))
          .withColumn("pascal", 'description.contains("pascal").cast(IntegerType))
          .withColumn("perl", 'description.contains("perl").cast(IntegerType))
          .withColumn("php", 'description.contains("php").cast(IntegerType))
          .withColumn("powershell", 'description.contains("powershell").cast(IntegerType))
          .withColumn("python", 'description.contains("python").cast(IntegerType))
          .withColumn("ruby", 'description.contains("ruby").cast(IntegerType))
          .withColumn("rust", 'description.contains("rust").cast(IntegerType))
          .withColumn("scala", 'description.contains("scala").cast(IntegerType))
          .withColumn("sql", 'description.contains("sql").cast(IntegerType))
          .withColumn("typescript", 'description.contains("typescript").cast(IntegerType))
          .withColumn("visual_basic", 'description.contains("visual basic").cast(IntegerType))
          .withColumn("wolfram", 'description.contains("wolfram").cast(IntegerType))
          .groupBy('year, 'month)
          .agg(
            sum("ada"),         sum("android"),       sum("clojure"),
            sum("cobol"),       sum("dart"),          sum("delphi"),
            sum("fortran"),     sum("ios"),           sum("java"),
            sum("javascript"),  sum("kotlin"),        sum("labview"),
            sum("matlab"),      sum("pascal"),        sum("perl"),
            sum("php"),         sum("powershell"),    sum("python"),
            sum("ruby"),        sum("scala"),         sum("sql"),
            sum("typescript"),  sum("visual_basic"),  sum("wolfram"))
          .withColumnRenamed("sum(ada)", "ada")
          .withColumnRenamed("sum(android)", "android")
          .withColumnRenamed("sum(clojure)", "clojure")
          .withColumnRenamed("sum(cobol)", "cobol")
          .withColumnRenamed("sum(dart)", "dart")
          .withColumnRenamed("sum(delphi)", "delphi")
          .withColumnRenamed("sum(fortran)", "fortran")
          .withColumnRenamed("sum(ios)", "ios")
          .withColumnRenamed("sum(java)", "java")
          .withColumnRenamed("sum(javascript)", "javascript")
          .withColumnRenamed("sum(kotlin)", "kotlin")
          .withColumnRenamed("sum(labview)", "labview")
          .withColumnRenamed("sum(matlab)", "matlab")
          .withColumnRenamed("sum(pascal)", "pascal")
          .withColumnRenamed("sum(perl)", "perl")
          .withColumnRenamed("sum(php)", "php")
          .withColumnRenamed("sum(powershell)", "powershell")
          .withColumnRenamed("sum(python)", "python")
          .withColumnRenamed("sum(ruby)", "ruby")
          .withColumnRenamed("sum(scala)", "scala")
          .withColumnRenamed("sum(sql)", "sql")
          .withColumnRenamed("sum(typescript)", "typescript")
          .withColumnRenamed("sum(visual_basic)", "visual_basic")
          .withColumnRenamed("sum(wolfram)", "wolfram")
          .drop("local_date").drop("id").drop("description")
          .orderBy('year, 'month)

      println("Saving analysis results to temp file...")
      Q3_base_df.write.option("header", "true").csv("output/temp/Q3_results")
      outputCombiner("output/temp/Q3_results", "output/question_03" , "results")

      println("Cleaning up results DataFrame...")
      Q3_base_df.unpersist()

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis4(): Unit = {
      println("Analysis 4 initialized...")

      println("Calculating number of events by location...")
      val Q04_location_df =
        df.select('localized_location)
          .groupBy('localized_location)
          .count()
          .orderBy('count.desc)

      Q04_location_df.show(10)


      Q04_location_df.write.csv("output/temp/Q4_top_locations")
      outputCombiner("output/temp/Q4_top_locations", "output/question_04", "top_locations")

      println("Cleaning up events by location DataFrame...")
      Q04_location_df.unpersist()

      println("Calculating number of events by venue...")
      val Q04_venue_df: DataFrame =
        df.select('v_name, 'localized_location)
          .filter('v_name.isNotNull)
          .groupBy('v_name)
          .count()
          .orderBy('count.desc)

      Q04_venue_df.show(10)

      Q04_venue_df.write.option("header", "true").csv("output/temp/Q4_top_venues")
      outputCombiner("output/temp/Q4_top_venues", "output/question_04", "top_venues")

      println("Cleaning up events by venue DataFrame...")
      Q04_venue_df.unpersist()

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis5(): Unit = {
      println("Analysis 5 initialized...")

//       Topic List
//        2: Career & Business
//        4: Movements
//        6: Education
//       12: LGBTQ
//       15: Hobbies & Craft
//       32: Sports and Fitness
//       36: Writing

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

      Q5_base_df.show(10)

      println("Saving analysis results to temp file...")
      Q5_base_df.write.option("header", "true").csv("output/temp/Q5_results")
      outputCombiner("output/temp/Q5_results", "output/question_05" , "topic_mentions_over_time")

      println("Cleaning up DataFrames...")
      Q5_base_df.unpersist()

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis6(): Unit = {
      println("Analysis 6 initialized...")

      println("Calculating the creation time of each event...")
      val Q6_byCount =
        df.filter(
            'created.isNotNull && 'millis_adjustment.isNotNull )
          .withColumn("minute_of_day_created", min_of_dayUDF('created + 'millis_adjustment))
          .groupBy('minute_of_day_created)
          .count()
          .orderBy('count.desc)

      Q6_byCount.show(10)

      println("Writing the ranked dataset to temp file...")
      Q6_byCount.write.option("header", "true").csv("output/temp/Q6_byCount")
      outputCombiner("output/temp/Q6_byCount", "output/question_06", "by_count" )

      println("Writing the chronological dataset to temp file...")
      Q6_byCount
        .orderBy('minute_of_day_created)     // Reordering the dataset to chronological order
        .write.option("header", "true").csv("output/temp/Q6_chronological")
      outputCombiner("output/temp/Q6_chronological", "output/question_06", "by_minute" )

      println("Cleaning up DataFrames...")
      Q6_byCount.unpersist()

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis7(): Unit = {
      println("Analysis 7 initialized...")

      val Q7_byCount =
        df
          .select('duration)
          .filter('duration.isNotNull)
          .withColumn("mins_duration", ('duration/60000).cast("int"))
          .drop('duration)
          .groupBy('mins_duration)
          .count()
          .orderBy('count.desc)
          .limit(20)

      Q7_byCount.show(10)

      println("Saving analysis results to temp file...")
      Q7_byCount.write.option("header", "true").csv("output/temp/Q7_byCount")
      outputCombiner("output/temp/Q7_byCount", "output/question_07", "by_count")

      println("Cleaning up DataFrame...")
      Q7_byCount.unpersist()

      val Q7_fullSet =
        df
          .select('duration)
          .filter( 'duration.isNotNull )
          .withColumn( "mins_duration", ('duration / 60000).cast(IntegerType) )
          .groupBy( 'mins_duration )
          .count()
          .orderBy( 'mins_duration )

      Q7_fullSet.show(10)

      println("Writing chronological data set for the first 24 hours to temp file...")
      Q7_fullSet
        .select('mins_duration, 'count)
        .filter('mins_duration <= 1440)
        .write.option("header", "true").csv("output/temp/Q7_firstDay")
      outputCombiner( "output/temp/Q7_firstDay", "output/question_07", "first_day" )

      // Full set of results
      println("Writing full chronological data set to temp file...")

      Q7_fullSet
        .select( 'mins_duration, 'count )
        .write.option("header", "true").csv("output/temp/Q7_fullSet")
      outputCombiner("output/temp/Q7_fullSet", "output/question_07", "full_set")

      println("Cleaning up DataFrames...")
      Q7_fullSet.unpersist()

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis8(): Unit = {
      println("Analysis 8 initialized...")

      println("Filtering base DataFrame for analysis...")
      val Q08_df =
        df.select('name, 'group_name, 'v_name, 'localized_location, 'yes_rsvp_count)
          .orderBy('yes_rsvp_count.desc)
          .limit(5)

      Q08_df.show(5)

      println("Saving analysis results to temp file...")
      Q08_df.write.option("header", "true").csv("output/temp/Q08_top_rsvps")
      outputCombiner("output/temp/Q08_top_rsvps", "output/question_08" , "top_rsvps")

      println("Cleaning up DataFrames...")
      Q08_df.unpersist()

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis9(): Unit = {
      println("Analysis 9 initialized...")

      val Q09_base_df =
        df.select('yes_rsvp_count, 'rsvp_limit, 'local_date)
          .filter('yes_rsvp_count.isNotNull || 'rsvp_limit.isNotNull)
          .na.fill(0)
          .withColumn("event_count", 'yes_rsvp_count + 'rsvp_limit)
          .withColumn("year", 'local_date.substr(0,4).cast(IntegerType))
          .withColumn("month", 'local_date.substr(6,2).cast(IntegerType))
          .groupBy('year, 'month).sum("event_count")
          .withColumnRenamed("sum(event_count)", "daily_event_attends")
          .drop('yes_rsvp_count).drop('rsvp_limit).drop('local_date)
          .orderBy('year, 'month)

      println("Saving analysis results to temp file...")
      Q09_base_df.write.option("header", "true").csv("output/temp/Q09")
      outputCombiner("output/temp/Q09", "output/question_09" , "daily_event_attendance")

      println("Cleaning up DataFrames...")
      Q09_base_df.unpersist()

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis10(): Unit = {
      println("Analysis 10 initialized...")

      println("Filtering and calculating...")
      val Q10_base_df =
        df.select('accepts, 'local_date)
          .filter('accepts.isNotNull || 'local_date.isNotNull)
          .withColumn("year", 'local_date.substr(0,4).cast(IntegerType))
          .withColumn("month", 'local_date.substr(6,2).cast(IntegerType))
          .withColumn("paypal", 'accepts.contains("paypal").cast(IntegerType))
          .withColumn("cash", 'accepts.contains("cash").cast(IntegerType))
          .withColumn("wepay", 'accepts.contains("wepay").cast(IntegerType))
          .filter('paypal.isNotNull || 'cash.isNotNull || 'wepay.isNotNull)
          .groupBy('year, 'month).sum("paypal", "cash", "wepay")
          .drop("paypal").drop("cash").drop("wepay").drop('accepts).drop('local_date)
          .withColumnRenamed("sum(paypal)", "paypal")
          .withColumnRenamed("sum(cash)", "cash")
          .withColumnRenamed("sum(wepay)", "wepay")
          .orderBy('year, 'month)

      println("Showing sample of results...")
      Q10_base_df.show(10)

      println("Saving analysis results to temp file...")
      Q10_base_df.write.option("header", "true").csv("output/temp/Q10")
      outputCombiner("output/temp/Q10", "output/question_10", "daily_accepted_pay_methods")

      println("Cleaning up DataFrames...")
      Q10_base_df.unpersist()

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis11(): Unit = {
      println("Analysis 11 initialized...")

      println("Filtering and calculating...")
      val Q11_df =
        df.select('amount, 'local_date)
          .filter('local_date.isNotNull)
          .na.fill(0)
          .withColumn("year", 'local_date.substr(0,4).cast(IntegerType))
          .withColumn("month", 'local_date.substr(6,2).cast(IntegerType))
          .groupBy('year, 'month).avg("amount")
          .withColumnRenamed("avg(amount)", "avg_cost")
          .orderBy('year, 'month)

      println("Showing sample of results...")
      Q11_df.show(10)

      println("Saving analysis results to temp file...")
      Q11_df.write.option("header", "true").csv("output/temp/Q11")
      outputCombiner("output/temp/Q11", "output/question_11" , "avg_attendance_cost")

      println("Cleaning up results DataFrame...")
      Q11_df.unpersist()

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis12(): Unit = {

      println("Analysis 12 initialized...")

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

    def analysis13(): Unit = {
      println("Analysis 13 initialized...")

      val Q13_df =
        df.select('group_name, 'yes_rsvp_count, 'rsvp_limit)
          .na.fill(0)
          .groupBy('group_name).agg(sum("yes_rsvp_count"), sum("rsvp_limit"), count("rsvp_limit"))
          .withColumnRenamed("sum(yes_rsvp_count)", "rsvps")
          .withColumnRenamed("sum(rsvp_limit)", "limit")
          .withColumnRenamed("count(rsvp_limit)", "count")
          .orderBy('count.desc, 'rsvps.desc, 'limit.desc)

      println("Showing sample of results...")
      Q13_df.show(10)

      println("Saving analysis results to temp file...")
      Q13_df.write.option("header", "true").csv("output/temp/Q13_results")
      outputCombiner("output/temp/Q13_results", "output/question_13", "largest_groups")

      println("Cleaning up results DataFrame...")
      Q13_df.unpersist()

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

    def outputCombiner(inPath: String, outPath: String, title: String, header: Boolean = true): Unit = {
      // Ensuring the output path is a correctly named filetype, ending in either .tsv or .csv.
      println(s"Moving temp data files to: $outPath/$title.csv")

      // Opening the home directory
      val directory = new File(inPath)

      // Creating a list of all .csv files in the input directory
      val csv_files = directory
        .listFiles()
        .filter(_.toString.endsWith(".csv"))

      // creating a file writer object to write each partial csv into one temp.csv
      val file = new File(s"$inPath/temp.txt")
      val writer = new PrintWriter(new FileWriter(file))

      // whether or not the csv files have headers, the first needs to be written
      // line for line (i.e. if it has headers, the headers need to be written)
      val bufferedSource = scala.io.Source.fromFile(csv_files(0))
      val lines = bufferedSource.getLines()
      for (line <- lines) {
        writer.write(line)
        writer.write('\n')
      }
      bufferedSource.close()

      // now loop through every line in every csv_file other than the first one
      for (csv_file <- csv_files.drop(1)) {
        // opening the file as a buffered source, reading the lines from it, then writing
        // the lines to the buffered writer object
        val bufferedSource = scala.io.Source.fromFile(csv_file)
        // if the header flag is active, then the first line of every csv file can be
        // skipped, otherwise just write every line to the composite
        if (header) {
          for (line <- bufferedSource.getLines().drop(1)) {
            writer.write(line)
            writer.write('\n')
          }
        } else {
          for (line <- bufferedSource.getLines()) {
            writer.write(line)
            writer.write('\n')
          }
        }
        bufferedSource.close()
      }
      writer.close()

      // creating the desired output directory then moving the temp file to the desired
      // output directory and renaming the temp file to the desired title.
      new File(outPath).mkdirs()
      val temp_output = new File(s"$inPath/temp.txt")
      temp_output.renameTo(new File(s"$outPath/$title.csv"))
    }
  }
}
