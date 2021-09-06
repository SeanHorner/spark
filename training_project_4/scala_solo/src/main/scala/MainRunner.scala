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

import scala.io.StdIn
import java.io.File
import java.util.Date

object MainRunner extends App {

  printWelcome()
  println()

  // Clearing up any previous output directories
  def recursive_delete(file: File): Unit = {
    if(file.isDirectory) {
      file.listFiles.foreach(recursive_delete)
    }
  }

//  val output = new File("output")
//  recursive_delete(output)

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
      analysesToRun = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)
    else
      analysesToRun = userInputList.map(_.toInt)

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
    + "|   5.  What are some of the most common event topics?                                                      |\n" // <-
    + "|   6.  What is the most popular time when events are created?                                              |\n"
    + "|   7.  Are events with longer durations more popular than shorter durations?                               |\n"
    + "|   8.  Which events have the most RSVPs?                                                                   |\n"
    + "|   9.  How has event capacity changed over the months/years?                                               |\n" // <-
    + "|  10.  What is the preferred payment method for events?                                                    |\n" // <-
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

    // Here initializing the SparkContext for the
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

    // UDF to find out which  [Analysis 3]
    def tech_mentions(desc: String): String = {
      var tech_seq = ""
      val tech_list = List(
        "ada", "android", "clojure", "cobol", "dart", "delphi", "fortran", "ios", "java", "javascript",
        "kotlin", "labview", "matlab", "pascal", "perl", "php", "powershell", "python", "ruby", "rust",
        "scala", "sql", "typescript", "visual basic", "wolfram"
      )
      for(tech <- tech_list)
        if(desc.toLowerCase.split(" ").map(_.trim).contains(tech))
          tech_seq += s"$tech "

      tech_seq
    }

    val tech_mentionsUDF: UserDefinedFunction = udf[String, String](tech_mentions)

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

    // Initial data set read in from Parquet file
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
//      new File("output/question_01/").mkdirs()
      outputCombiner("output/temp/Q1_results", "output/question_01", "results")

      println("Cleaning up DataFrames...")
      Q1_results_df.unpersist()

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis2(): Unit = {
      println("Analysis 2 initialized...")

      val Q2_base_df =
        df.select('id, 'local_date, 'is_online_event)
          .withColumn("year", 'local_date.substr(0,4).cast(IntegerType))

      var results = Seq[Row]()

      for (y <- 2003 to 2020) {
        var temp_seq = Seq[Int](y)

        println(s"Testing year $y...")
        temp_seq = temp_seq :+ Q2_base_df
          .filter('year === y)
          .filter('is_online_event === true)
          .count()
          .toInt

        temp_seq = temp_seq :+ Q2_base_df
          .filter('year === y)
          .filter('is_online_event === false)
          .count()
          .toInt

        results = results :+ Row.fromSeq(temp_seq)
      }

      val Q2_results_schema = StructType(
        List(
          StructField("year", IntegerType, nullable = false),
          StructField("in_person", IntegerType, nullable = false),
          StructField("online", IntegerType, nullable = false)))
      val Q2_results_rdd = spark.sparkContext.parallelize(results)
      val Q2_results_df = spark.createDataFrame(Q2_results_rdd, Q2_results_schema).orderBy('year)

      Q2_results_df.show()

      Q2_results_df.write.csv("output/temp/Q2_results")
//      new File("output/question_02/").mkdirs()
      outputCombiner("output/temp/Q2_results", "output/question_02", "results")

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

      println("Building base DataFrame...")
      val Q3_base_df =
        df.select('id, 'local_date, 'description)
          .withColumn("year", 'local_date.substr(0,4).cast(IntegerType))
          .withColumn("month", 'local_date.substr(6,2).cast(IntegerType))
          .withColumn("tech_mentioned", tech_mentionsUDF('description))

      var results_base = Seq[Row]()

      val tech_list = List(
        "ada", "android", "clojure", "cobol", "dart", "delphi", "fortran", "ios", "java", "javascript",
        "kotlin", "labview", "matlab", "pascal", "perl", "php", "powershell", "python", "ruby", "rust",
        "scala", "sql", "typescript", "visual basic", "wolfram"
      )

      // Looping through the years in the dataset
      for (y <- 2003 to 2020) {
        var temp_seq = Seq[Int](y)

        println(s"Narrowing data set to year $y...")
        // Looping through the technologies...
        for (tech <- tech_list) {
          println(s"\tCounting $tech...")
          temp_seq = temp_seq :+ Q3_base_df
            .filter('year === y)
            .filter('tech_mentioned.contains(tech))
            .count()
            .toInt
        }

        results_base = results_base :+ Row.fromSeq(temp_seq)
      }

      // Done with base DataFrame
      Q3_base_df.unpersist()

      val Q3_results_schema = StructType(
        List(
          StructField("year", IntegerType, nullable = false),
          StructField("ada", IntegerType, nullable = false),
          StructField("android", IntegerType, nullable = false),
          StructField("clojure", IntegerType, nullable = false),
          StructField("cobol", IntegerType, nullable = false),
          StructField("dart", IntegerType, nullable = false),
          StructField("delphi", IntegerType, nullable = false),
          StructField("fortran", IntegerType, nullable = false),
          StructField("ios", IntegerType, nullable = false),
          StructField("java", IntegerType, nullable = false),
          StructField("javascript", IntegerType, nullable = false),
          StructField("kotlin", IntegerType, nullable = false),
          StructField("labview", IntegerType, nullable = false),
          StructField("matlab", IntegerType, nullable = false),
          StructField("pascal", IntegerType, nullable = false),
          StructField("perl", IntegerType, nullable = false),
          StructField("php", IntegerType, nullable = false),
          StructField("powershell", IntegerType, nullable = false),
          StructField("python", IntegerType, nullable = false),
          StructField("ruby", IntegerType, nullable = false),
          StructField("rust", IntegerType, nullable = false),
          StructField("scala", IntegerType, nullable = false),
          StructField("sql", IntegerType, nullable = false),
          StructField("typescript", IntegerType, nullable = false),
          StructField("visual_basic", IntegerType, nullable = false),
          StructField("wolfram", IntegerType, nullable = false),
        )
      )

      val Q3_results_rdd = spark.sparkContext.parallelize(results_base)
      Q3_results_rdd.foreach(row => println(row.toString()))


      val Q3_results_df = spark.createDataFrame(Q3_results_rdd, Q3_results_schema).orderBy('year)

      Q3_results_df.show()

      Q3_results_df.write.csv("output/temp/Q3_results")
//      new File("output/question_03/").mkdirs()
      outputCombiner("output/temp/Q3_results", "output/question_03" , "results")

      println("Cleaning up results DataFrame...")
      Q3_results_df.unpersist()

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis4(): Unit = {
      println("Analysis 4 initialized...")

      println("Calculating number of events by location...")
      val eventCountByLocationDf =
        df.select('localized_location)
          .groupBy('localized_location)
          .count()
          .orderBy('count.desc)

      eventCountByLocationDf.show(10)


      eventCountByLocationDf.write.csv("output/temp/Q4_top_locations")
//      new File("output/question_04/").mkdirs()
      outputCombiner("output/temp/Q4_top_locations", "output/question_04", "top_locations")

      println("Cleaning up events by location DataFrame...")
      eventCountByLocationDf.unpersist()

      println("Calculating number of events by venue...")
      val eventCountByVenueDf: DataFrame =
        df.select('v_name, 'localized_location)
          .filter('v_name.isNotNull)
          .groupBy('v_name)
          .count()
          .orderBy('count.desc)

      eventCountByVenueDf.show(10)

      eventCountByVenueDf.write.csv("output/temp/Q4_top_venues")
      outputCombiner("output/temp/Q4_top_venues", "output/question_04", "top_venues")

      println("Cleaning up events by venue DataFrame...")
      eventCountByVenueDf.unpersist()

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis5(): Unit = {
      println("Analysis 5 initialized...")

      /**
       * Topic List
       *  2: Career & Business
       *  4: Movements
       *  6: Education
       * 12: LGBTQ
       * 15: Hobbies & Craft
       * 32: Sports and Fitness
       * 36: Writing
       */
      val Q4_base_df = df
        .select('id, 'category_ids)
        .withColumn("category_ids", category_array_converterUDF('category_ids))
        .withColumn("category_count", category_array_counterUDF('category_ids))
        .filter('category_count.isNotNull)
      //    .filter('category_count === 2)

      Q4_base_df.show(10)
      Q4_base_df.printSchema()
      println(Q4_base_df.count())

      println("Cleaning up DataFrames...")
      Q4_base_df.unpersist()

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

      println("Writing the ranked dataset to temp file...")
      Q6_byCount.write.csv("output/temp/Q6_byCount")
//      new File("output/question_06/").mkdirs()
      outputCombiner("output/temp/Q6_byCount", "output/question_06", "by_count" )

      println("Writing the chronological dataset to temp file...")
      Q6_byCount
        .orderBy('minute_of_day_created)     // Reordering the dataset to chronological order
        .write.csv("output/temp/Q6_chronological")
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
          .filter( 'duration.isNotNull )
          .withColumn( "mins_duration", ('duration/60000).cast("int") )
          .drop( 'duration )
          .groupBy( 'mins_duration )
          .count()
          .orderBy( 'count.desc )
          .limit( 20 )

      Q7_byCount.show(20)

      println("Writing results to temp file...")
      Q7_byCount.write.csv("output/temp/Q7_byCount")
//      new File("output/question_07/").mkdirs()
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

      println("Writing chronological data set for the first 24 hours to temp file...")
      Q7_fullSet
        .select('mins_duration, 'count)
        .filter('mins_duration <= 1440)
        .write.csv("output/temp/Q7_firstDay")
      outputCombiner( "output/temp/Q7_firstDay", "output/question_07", "first_day" )

      // Full set of results
      println("Writing full chronological data set to temp file...")

      Q7_fullSet
        .select( 'mins_duration, 'count )
        .write.csv("output/temp/Q7_fullSet")
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
      val Q8_df =
        df.select('name, 'group_name, 'v_name, 'localized_location, 'yes_rsvp_count)
          .orderBy('yes_rsvp_count.desc)
          .limit(5)

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("Saving analysis results...")
      Q8_df.write.csv("output/temp/Q08_top_rsvps")
//      new File("output/question_08/").mkdirs()
      outputCombiner("output/temp/Q08_top_rsvps", "output/question_08" , "top_rsvps")

      Q8_df.show()

      println("Cleaning up DataFrames...")
      Q8_df.unpersist()

      println("*** Analysis finished. ***\n\n")
    }

    def analysis9(): Unit = {
      println("Analysis 9 initialized...")



      println("Cleaning up DataFrames...")

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis10(): Unit = {
      println("Analysis 10 initialized...")



      println("Cleaning up DataFrames...")

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis11(): Unit = {
      println("Analysis 11 initialized...")

      val Q11_base_df = df
        .select('id, 'local_date, 'accepts, 'amount)
        .filter('accepts.isNotNull)
        .withColumn("year", 'local_date.substr(0, 4).cast(IntegerType))

      var results_base = Seq[Row]()
      for (y <- 2003 to 2020) {
        var temp_seq = Seq[Int](y)
        for(option <- List("cash", "paypal", "wepay")) {
          temp_seq = temp_seq :+ Q11_base_df
            .filter('year === y)
            .filter('accepts === option)
            .count()
            .toInt
        }
        results_base = results_base :+ Row.fromSeq(temp_seq)
      }

      println("Cleaning up base DataFrame...")
      Q11_base_df.unpersist()

      val Q11_results_schema = StructType(
        List(
          StructField("year", IntegerType, nullable = false),
          StructField("cash", IntegerType, nullable = false),
          StructField("paypal", IntegerType, nullable = false),
          StructField("wepay", IntegerType, nullable = false)
        )
      )

      val Q11_results_rdd = spark.sparkContext.parallelize(results_base)
      val Q11_results_df = spark.createDataFrame(Q11_results_rdd, Q11_results_schema).orderBy('year)

      Q11_results_df.show()

      println("Writing results to temp output file...")
      Q11_results_df.write.csv("output/temp/Q11_results")
      outputCombiner("output/temp/Q11_results", "output/question_11" , "results")

      println("Cleaning up results DataFrame...")
      Q11_results_df.unpersist()

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

      val Q12_DF = Seq(
        (2003, Q12List(0)),  (2004, Q12List(1)),  (2005, Q12List(2)),
        (2006, Q12List(3)),  (2007, Q12List(4)),  (2008, Q12List(5)),
        (2009, Q12List(6)),  (2010, Q12List(7)),  (2011, Q12List(8)),
        (2012, Q12List(9)),  (2013, Q12List(10)), (2014, Q12List(11)),
        (2015, Q12List(12)), (2016, Q12List(13)), (2017, Q12List(14)),
        (2018, Q12List(15)), (2019, Q12List(16)), (2020, Q12List(17)),
      ).toDF("year", "avg_prep_time")

      Q12_DF.show()

      println("Writing results to temp file...")
      Q12_DF.write.csv("output/temp/Q12_results")
      outputCombiner("output/temp/Q12_results", "output/question_12", "full_set")

      println("Cleaning up results DataFrame...")
      Q12_DF.unpersist()

      println("Beginning visualization creation...")
      /**
       * Data visualization logic goes here
       */

      println("*** Analysis finished. ***\n\n")
    }

    def analysis13(): Unit = {
      println("Analysis 13 initialized...")



      println("Cleaning up DataFrames...")

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
}
