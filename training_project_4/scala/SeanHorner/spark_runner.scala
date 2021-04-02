package SeanHorner

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import java.io.File

object spark_runner {
  def mainRunner(): Unit = {
    val spark = SparkSession.builder()
      .appName("Meetup Trends")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    meetup_analysis(spark)

  }

  def meetup_analysis(spark: SparkSession): Unit = {
    import spark.implicits._

    val df = spark.read.parquet("all_cities_data.parquet")

    val millisToMin = udf[Int, Long](milToMin)
    val tzAdj = udf[Long, String](timezoneAdj)
    def tt(tech: String, year: Int): Int = {
      val startMillis: Long = (year - 1970)*31536000000L
      val endMillis: Long = (year + 1 - 1970)*31536000000L

      df
        .filter('description.contains(tech))
        .filter('time >= startMillis && 'time <= endMillis)
        .count().toInt
    }


    //-------------------------------------------------------------------------------------------------------

    // Beginning of Question 1 analysis.
    // What is the trend of new tech vs established tech meetups? (e.g. number of meetups about C++ vs. Rust)

    val Q1DF =
      Seq(
      ("Ada", 1983, tt("ada ", 2003), tt("ada ", 2004), tt("ada ", 2005),tt("ada ", 2006),tt("ada ", 2007),
        tt("ada ", 2008),tt("ada ", 2009),tt("ada ", 2010),tt("ada ", 2011),tt("ada ", 2012),
        tt("ada ", 2013),tt("ada ", 2014),tt("ada ", 2015),tt("ada ", 2016),tt("ada ", 2017),
        tt("ada ", 2018),tt("ada ", 2019),tt("ada ", 2020)),
      ("Android", 2008, tt("android ", 2003), tt("android ", 2004), tt("android ", 2005),tt("android ", 2006),tt("android ", 2007),
        tt("android ", 2008),tt("android ", 2009),tt("android ", 2010),tt("android ", 2011),tt("android ", 2012),
        tt("android ", 2013),tt("android ", 2014),tt("android ", 2015),tt("android ", 2016),tt("android ", 2017),
        tt("android ", 2018),tt("android ", 2019),tt("android ", 2020)),
      ("Clojure", 2007, tt("clojure ", 2003), tt("clojure ", 2004), tt("clojure ", 2005),tt("clojure ", 2006),tt("clojure ", 2007),
        tt("clojure ", 2008),tt("clojure ", 2009),tt("clojure ", 2010),tt("clojure ", 2011),tt("clojure ", 2012),
        tt("clojure ", 2013),tt("clojure ", 2014),tt("clojure ", 2015),tt("clojure ", 2016),tt("clojure ", 2017),
        tt("clojure ", 2018),tt("clojure ", 2019),tt("clojure ", 2020)),
      ("COBOL", 1959, tt("cobol ", 2003), tt("cobol ", 2004), tt("cobol ", 2005),tt("cobol ", 2006),tt("cobol ", 2007),
        tt("cobol ", 2008),tt("cobol ", 2009),tt("cobol ", 2010),tt("cobol ", 2011),tt("cobol ", 2012),
        tt("cobol ", 2013),tt("cobol ", 2014),tt("cobol ", 2015),tt("cobol ", 2016),tt("cobol ", 2017),
        tt("cobol ", 2018),tt("cobol ", 2019),tt("cobol ", 2020)),
      ("Dart", 2011, tt("dart ", 2003), tt("dart ", 2004), tt("dart ", 2005),tt("dart ", 2006),tt("dart ", 2007),
        tt("dart ", 2008),tt("dart ", 2009),tt("dart ", 2010),tt("dart ", 2011),tt("dart ", 2012),
        tt("dart ", 2013),tt("dart ", 2014),tt("dart ", 2015),tt("dart ", 2016),tt("dart ", 2017),
        tt("dart ", 2018),tt("dart ", 2019),tt("dart ", 2020)),
      ("Delphi", 1995, tt("delphi ", 2003), tt("delphi ", 2004), tt("delphi ", 2005),tt("delphi ", 2006),tt("delphi ", 2007),
        tt("delphi ", 2008),tt("delphi ", 2009),tt("delphi ", 2010),tt("delphi ", 2011),tt("delphi ", 2012),
        tt("delphi ", 2013),tt("delphi ", 2014),tt("delphi ", 2015),tt("delphi ", 2016),tt("delphi ", 2017),
        tt("delphi ", 2018),tt("delphi ", 2019),tt("delphi ", 2020)),
      ("FORTRAN", 1957, tt("fortran ", 2003), tt("fortran ", 2004), tt("fortran ", 2005),tt("fortran ", 2006),tt("fortran ", 2007),
        tt("fortran ", 2008),tt("fortran ", 2009),tt("fortran ", 2010),tt("fortran ", 2011),tt("fortran ", 2012),
        tt("fortran ", 2013),tt("fortran ", 2014),tt("fortran ", 2015),tt("fortran ", 2016),tt("fortran ", 2017),
        tt("fortran ", 2018),tt("fortran ", 2019),tt("fortran ", 2020)),
      ("iOS", 2007, tt("ios ", 2003), tt("ios ", 2004), tt("ios ", 2005),tt("ios ", 2006),tt("ios ", 2007),
        tt("ios ", 2008),tt("ios ", 2009),tt("ios ", 2010),tt("ios ", 2011),tt("ios ", 2012),
        tt("ios ", 2013),tt("ios ", 2014),tt("ios ", 2015),tt("ios ", 2016),tt("ios ", 2017),
        tt("ios ", 2018),tt("ios ", 2019),tt("ios ", 2020)),
      ("Java", 1995, tt("java ", 2003), tt("java ", 2004), tt("java ", 2005),tt("java ", 2006),tt("java ", 2007),
        tt("java ", 2008),tt("java ", 2009),tt("java ", 2010),tt("java ", 2011),tt("java ", 2012),
        tt("java ", 2013),tt("java ", 2014),tt("java ", 2015),tt("java ", 2016),tt("java ", 2017),
        tt("java ", 2018),tt("java ", 2019),tt("java ", 2020)),
      ("JavaScript", 1995, tt("javascript ", 2003), tt("javascript ", 2004), tt("javascript ", 2005),tt("javascript ", 2006),tt("javascript ", 2007),
        tt("javascript ", 2008),tt("javascript ", 2009),tt("javascript ", 2010),tt("javascript ", 2011),tt("javascript ", 2012),
        tt("javascript ", 2013),tt("javascript ", 2014),tt("javascript ", 2015),tt("javascript ", 2016),tt("javascript ", 2017),
        tt("javascript ", 2018),tt("javascript ", 2019),tt("javascript ", 2020)),
      ("Kotlin", 2011, tt("kotlin ", 2003), tt("kotlin ", 2004), tt("kotlin ", 2005),tt("kotlin ", 2006),tt("kotlin ", 2007),
        tt("kotlin ", 2008),tt("kotlin ", 2009),tt("kotlin ", 2010),tt("kotlin ", 2011),tt("kotlin ", 2012),
        tt("kotlin ", 2013),tt("kotlin ", 2014),tt("kotlin ", 2015),tt("kotlin ", 2016),tt("kotlin ", 2017),
        tt("kotlin ", 2018),tt("kotlin ", 2019),tt("kotlin ", 2020)),
      ("LabVIEW", 1986, tt("labview ", 2003), tt("labview ", 2004), tt("labview ", 2005),tt("labview ", 2006),tt("labview ", 2007),
        tt("labview ", 2008),tt("labview ", 2009),tt("labview ", 2010),tt("labview ", 2011),tt("labview ", 2012),
        tt("labview ", 2013),tt("labview ", 2014),tt("labview ", 2015),tt("labview ", 2016),tt("labview ", 2017),
        tt("labview ", 2018),tt("labview ", 2019),tt("labview ", 2020)),
      ("MATLAB", 1984, tt("matlab ", 2003), tt("matlab ", 2004), tt("matlab ", 2005),tt("matlab ", 2006),tt("matlab ", 2007),
        tt("matlab ", 2008),tt("matlab ", 2009),tt("matlab ", 2010),tt("matlab ", 2011),tt("matlab ", 2012),
        tt("matlab ", 2013),tt("matlab ", 2014),tt("matlab ", 2015),tt("matlab ", 2016),tt("matlab ", 2017),
        tt("matlab ", 2018),tt("matlab ", 2019),tt("matlab ", 2020)),
      ("Pascal", 1970, tt("pascal ", 2003), tt("pascal ", 2004), tt("pascal ", 2005),tt("pascal ", 2006),tt("pascal ", 2007),
        tt("pascal ", 2008),tt("pascal ", 2009),tt("pascal ", 2010),tt("pascal ", 2011),tt("pascal ", 2012),
        tt("pascal ", 2013),tt("pascal ", 2014),tt("pascal ", 2015),tt("pascal ", 2016),tt("pascal ", 2017),
        tt("pascal ", 2018),tt("pascal ", 2019),tt("pascal ", 2020)),
      ("Perl", 1987, tt("perl ", 2003), tt("perl ", 2004), tt("perl ", 2005),tt("perl ", 2006),tt("perl ", 2007),
        tt("perl ", 2008),tt("perl ", 2009),tt("perl ", 2010),tt("perl ", 2011),tt("perl ", 2012),
        tt("perl ", 2013),tt("perl ", 2014),tt("perl ", 2015),tt("perl ", 2016),tt("perl ", 2017),
        tt("perl ", 2018),tt("perl ", 2019),tt("perl ", 2020)),
      ("PHP", 1995, tt("php ", 2003), tt("php ", 2004), tt("php ", 2005),tt("php ", 2006),tt("php ", 2007),
        tt("php ", 2008),tt("php ", 2009),tt("php ", 2010),tt("php ", 2011),tt("php ", 2012),
        tt("php ", 2013),tt("php ", 2014),tt("php ", 2015),tt("php ", 2016),tt("php ", 2017),
        tt("php ", 2018),tt("php ", 2019),tt("php ", 2020)),
      ("PowerShell", 2006, tt("powershell ", 2003), tt("powershell ", 2004), tt("powershell ", 2005),tt("powershell ", 2006),tt("powershell ", 2007),
        tt("powershell ", 2008),tt("powershell ", 2009),tt("powershell ", 2010),tt("powershell ", 2011),tt("powershell ", 2012),
        tt("powershell ", 2013),tt("powershell ", 2014),tt("powershell ", 2015),tt("powershell ", 2016),tt("powershell ", 2017),
        tt("powershell ", 2018),tt("powershell ", 2019),tt("powershell ", 2020)),
      ("Python", 1990, tt("python ", 2003), tt("python ", 2004), tt("python ", 2005),tt("python ", 2006),tt("python ", 2007),
        tt("python ", 2008),tt("python ", 2009),tt("python ", 2010),tt("python ", 2011),tt("python ", 2012),
        tt("python ", 2013),tt("python ", 2014),tt("python ", 2015),tt("python ", 2016),tt("python ", 2017),
        tt("python ", 2018),tt("python ", 2019),tt("python ", 2020)),
      ("Ruby", 1995, tt("ruby ", 2003), tt("ruby ", 2004), tt("ruby ", 2005),tt("ruby ", 2006),tt("ruby ", 2007),
        tt("ruby ", 2008),tt("ruby ", 2009),tt("ruby ", 2010),tt("ruby ", 2011),tt("ruby ", 2012),
        tt("ruby ", 2013),tt("ruby ", 2014),tt("ruby ", 2015),tt("ruby ", 2016),tt("ruby ", 2017),
        tt("ruby ", 2018),tt("ruby ", 2019),tt("ruby ", 2020)),
      ("Rust", 2010, tt("rust ", 2003), tt("rust ", 2004), tt("rust ", 2005),tt("rust ", 2006),tt("rust ", 2007),
        tt("rust ", 2008),tt("rust ", 2009),tt("rust ", 2010),tt("rust ", 2011),tt("rust ", 2012),
        tt("rust ", 2013),tt("rust ", 2014),tt("rust ", 2015),tt("rust ", 2016),tt("rust ", 2017),
        tt("rust ", 2018),tt("rust ", 2019),tt("rust ", 2020)),
      ("Scala", 2003, tt("scala ", 2003), tt("scala ", 2004), tt("scala ", 2005),tt("scala ", 2006),tt("scala ", 2007),
        tt("scala ", 2008),tt("scala ", 2009),tt("scala ", 2010),tt("scala ", 2011),tt("scala ", 2012),
        tt("scala ", 2013),tt("scala ", 2014),tt("scala ", 2015),tt("scala ", 2016),tt("scala ", 2017),
        tt("scala ", 2018),tt("scala ", 2019),tt("scala ", 2020)),
      ("SQL", 1978, tt("sql ", 2003), tt("sql ", 2004), tt("sql ", 2005),tt("sql ", 2006),tt("sql ", 2007),
        tt("sql ", 2008),tt("sql ", 2009),tt("sql ", 2010),tt("sql ", 2011),tt("sql ", 2012),
        tt("sql ", 2013),tt("sql ", 2014),tt("sql ", 2015),tt("sql ", 2016),tt("sql ", 2017),
        tt("sql ", 2018),tt("sql ", 2019),tt("sql ", 2020)),
      ("TypeScript", 2012, tt("typescript ", 2003), tt("typescript ", 2004), tt("typescript ", 2005),tt("typescript ", 2006),tt("typescript ", 2007),
        tt("typescript ", 2008),tt("typescript ", 2009),tt("typescript ", 2010),tt("typescript ", 2011),tt("typescript ", 2012),
        tt("typescript ", 2013),tt("typescript ", 2014),tt("typescript ", 2015),tt("typescript ", 2016),tt("typescript ", 2017),
        tt("typescript ", 2018),tt("typescript ", 2019),tt("typescript ", 2020)),
      ("Visual Basic", 1991, tt("visual  basic ", 2003), tt("visual  basic ", 2004), tt("visual  basic ", 2005),tt("visual  basic ", 2006),tt("visual  basic ", 2007),
        tt("visual  basic ", 2008),tt("visual  basic ", 2009),tt("visual  basic ", 2010),tt("visual  basic ", 2011),tt("visual  basic ", 2012),
        tt("visual  basic ", 2013),tt("visual  basic ", 2014),tt("visual  basic ", 2015),tt("visual  basic ", 2016),tt("visual  basic ", 2017),
        tt("visual  basic ", 2018),tt("visual  basic ", 2019),tt("visual  basic ", 2020)),
      ("Wolfram", 1988, tt("wolfram ", 2003), tt("wolfram ", 2004), tt("wolfram ", 2005),tt("wolfram ", 2006),tt("wolfram ", 2007),
        tt("wolfram ", 2008),tt("wolfram ", 2009),tt("wolfram ", 2010),tt("wolfram ", 2011),tt("wolfram ", 2012),
        tt("wolfram ", 2013),tt("wolfram ", 2014),tt("wolfram ", 2015),tt("wolfram ", 2016),tt("wolfram ", 2017),
        tt("wolfram ", 2018),tt("wolfram ", 2019),tt("wolfram ", 2020))
    ).toDF("Technology", "Year", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020")
      .orderBy('Year)

    Q1DF
      .drop('Year)
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .option("headers", "true")
      .save("question_1_data")

    outputConverter("question_1_data", "output/question1/question_1.tsv")

    Q1DF.unpersist()

    //-------------------------------------------------------------------------------------------------------

    // Beginning of Question 3 analysis.
    // What is the most popular time when events are created? (Find local_time/date)

    val Q3DF_byCount = df
      .filter('created.isNotNull && 'localized_location.isNotNull)
      .withColumn("timezoneAdjustment", tzAdj('localized_location))
      .withColumn("modulated_time_created",
        ('created+ 'timezoneAdjustment) % 86400000)
      .withColumn("min", millisToMin('modulated_time_created))
      .groupBy('min)
      .count()
      .orderBy('count.desc)
      .limit(20)

    Q3DF_byCount
      .select('min, 'count)
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_3_data_byCount")

    outputConverter("question_3_data_byCount", "output/question3/question_3_byCount.tsv")

    Q3DF_byCount.unpersist()

    // Full output for graphing
    val Q3DF = df
      .filter('created.isNotNull && 'localized_location.isNotNull)
      .withColumn("timezoneAdjustment", tzAdj('localized_location))
      .withColumn("modulated_time_created",
        ('created+ 'timezoneAdjustment) % 86400000)
      .withColumn("min", millisToMin('modulated_time_created))
      .groupBy('min)
      .count()
      .orderBy('min)

    Q3DF
      .select('min, 'count)
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_3_data")

    outputConverter("question_3_data", "output/question3/question_3.tsv")

    Q3DF.unpersist()

    //-------------------------------------------------------------------------------------------------------

    // Beginning of Question 5 analysis.
    // Are events with longer durations more popular vs shorter ones?

    // Ranking by count, top 10 values
    val Q5DF_byCount = df
      .filter(df("duration").isNotNull)
      .withColumn("min", millisToMin('duration))
      .groupBy('min)
      .count()
      .orderBy('count.desc)
      .limit(20)

    Q5DF_byCount
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_5_data_byCount")

    outputConverter("question_5_data_byCount", "output/question5/question_5_byCount.tsv")

    Q5DF_byCount.unpersist()

    // Values for up to 24 hours (first day)
    val Q5DF_first_day = df
      .filter(df("duration").isNotNull)
      .withColumn("min", millisToMin('duration))
      .groupBy('min)
      .count()
      .filter('min <= 1440)
      .orderBy('min)

    Q5DF_first_day
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_5_data_firstday")

    outputConverter("question_5_data_firstday", "output/question5/question_5_halfday.tsv")

    Q5DF_first_day.unpersist()

    // Full set of values after first day
    val Q5DF_fullset = df
      .filter(df("duration").isNotNull)
      .withColumn("min", millisToMin('duration))
      .groupBy('min)
      .count()
      .orderBy('min)
      .filter('min > 1440)

    Q5DF_fullset
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .save("question_5_data_fullset")

    outputConverter("question_5_data_fullset", "output/question5/question_5_full.tsv")

    Q5DF_fullset.unpersist()

    //-------------------------------------------------------------------------------------------------------

    // Beginning of Question 13 analysis.
    // Has there been a change in planning times for events? (time - created)
    def prepTime(year: Int): Long = {
      val startMillis: Long = (year - 1970)*31536000000L
      val endMillis: Long = (year + 1 - 1970)*31536000000L

      val prepDF = df
        .filter('created.isNotNull && 'time.isNotNull)
        .filter('time > startMillis)
        .filter('time <= endMillis)
        .withColumn("prepping_period", 'time - 'created)
        .withColumn("prep_min", millisToMin('prepping_period))
        .filter('prep_min >= 0)
        .groupBy('prep_min)
        .count()
        .withColumn("prep_time", 'prep_min * 'count)

      val total_prep_time: Long =
        prepDF.agg(sum('prep_time).cast("long")).first.getLong(0)

      val total_events =
        prepDF.agg(sum('count).cast("long")).first.getLong(0)

      val average_prep_time = total_prep_time/total_events

      prepDF.unpersist()

      average_prep_time
    }

    val Q13Arr = Array(
      0L, 0L, 0L, 0L, 0L, 0L,
      0L, 0L, 0L, 0L, 0L, 0L,
      0L, 0L, 0L, 0L, 0L, 0L)

    for(y <- 2003 to 2020) {
      Q13Arr(y-2003) = prepTime(y)
    }

    val Q13DF = Seq(
      (2003, Q13Arr(0)),
      (2004, Q13Arr(1)),
      (2005, Q13Arr(2)),
      (2006, Q13Arr(3)),
      (2007, Q13Arr(4)),
      (2008, Q13Arr(5)),
      (2009, Q13Arr(6)),
      (2010, Q13Arr(7)),
      (2011, Q13Arr(8)),
      (2012, Q13Arr(9)),
      (2013, Q13Arr(10)),
      (2014, Q13Arr(11)),
      (2015, Q13Arr(12)),
      (2016, Q13Arr(13)),
      (2017, Q13Arr(14)),
      (2018, Q13Arr(15)),
      (2019, Q13Arr(16)),
      (2020, Q13Arr(17))
    ).toDF("year", "avg_prep_time")

    Q13DF
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .option("headers", "true")
      .save("question_13_data")

    outputConverter("question_13_data", "output/question13/question_13_avgs.tsv")

  }

  //-------------------------------------------------------------------------------------------------------
  //--------------------------------------METHODS----------------------------------------------------------
  //-------------------------------------------------------------------------------------------------------

  def outputConverter(searchDir: String, tsvOutput: String): Unit = {
    val dir = new File(searchDir)
    val newFileRgex = ".*part-00000.*.csv"
    val tmpTsvFile = dir
      .listFiles
      .filter(_.toString.matches(newFileRgex))(0)
      .toString
    new File(tmpTsvFile).renameTo(new File(tsvOutput))

    dir.listFiles.foreach( f => f.delete )
    dir.delete
  }

  def milToMin(millis: Long): Int = (millis/ 60000).toInt

  def timezoneAdj(str: String): Long = analysis_helper.citiesTimeAdj(str)
}
