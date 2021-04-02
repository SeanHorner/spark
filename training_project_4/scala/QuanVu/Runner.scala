/*
Quan Vu, Staging Project - Meetup Tech Events Analysis
Questions 6 and 12
 */

package QuanVu

import com.cibo.evilplot.colors._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._


object Runner {
  def mainRunner(): Unit ={

    // Solved java.io exception error of missing winutils.exe
    //System.setProperty("hadoop.home.dir", "C:/Hadoop")

    // Start our Spark SQL entry point
    val spark = SparkSession.builder()
      .appName("Tech Meetups Trend")
      .master("local[4]")
      .getOrCreate()

    // Set console log to give warnings or worse
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Read parquet files and convert to dataframes
    // Change parquet reading path to the appropriate directory.
    val df = spark.read.parquet("input").createOrReplaceTempView("view1")
//      spark.read.option("header", "true")
//      .option("sep", "\t")
//      .csv("data_50cities_v3.tsv")
//      .createOrReplaceTempView("view1")

    // Find tech event counts for each year starting at 2002 to 2020
    val q1 = spark.sql("SELECT SUBSTRING(view1.local_date, 1, 4) AS year, COUNT(*) AS counts " +
      "FROM view1 " +
      "WHERE view1.local_date LIKE '2%' " +
      "AND view1.local_date <= '2021' " +
      "GROUP BY year " +
      "ORDER BY year ASC")//.show()

    // Store results into csv files
    q1.coalesce(1).write.format("com.databricks.spark.csv")
      .save("output/question6/yearly_events")

    //-----------------------------------------------------------------------------------------------------------------
    // Question 6: Find the tech event counts for each month in 2020
    val q2a = spark.sql("SELECT SUBSTRING(view1.local_date, 6, 2) AS months, COUNT(*) AS 2020_counts " +
      "FROM view1 " +
      "WHERE view1.local_date LIKE '2020%' " +
      "GROUP BY months " +
      "ORDER BY months")//.show()
    // Find the tech event counts for each month in 2019
    val q2b = spark.sql("SELECT SUBSTRING(view1.local_date, 6, 2) AS months, COUNT(*) AS 2019_counts " +
      "FROM view1 " +
      "WHERE view1.local_date LIKE '2019%' " +
      "GROUP BY months " +
      "ORDER BY months")//.show()
    // Find the tech event counts for each month in 2018
    val q2c = spark.sql("SELECT SUBSTRING(view1.local_date, 6, 2) AS months, COUNT(*) AS 2018_counts " +
      "FROM view1 " +
      "WHERE view1.local_date LIKE '2018%' " +
      "GROUP BY months " +
      "ORDER BY months")//.show()

    // Events created comparisons for 2020, 2019, and 2018.
    val q2 = q2a.join(q2b.join(q2c, "months"), "months").orderBy($"months".asc)

    q2.coalesce(1).write.format("com.databricks.spark.csv")
      .save("output/question6/monthly_events_2018to2020")

    //-----------------------------------------------------------------------------------------------------------------
    // Question 12: How has the capacity (total rsvp_limit) changed over time? Find total rsvp limit.
    // From 2005 to 2020
    val q3 = spark.sql("SELECT SUBSTRING(view1.local_date, 1, 4) AS year, " +
      "ROUND(COUNT(view1.rsvp_limit), 2) AS total_rsvp_limit " +
      "FROM view1 " +
      "WHERE view1.local_date LIKE '2%' AND view1.local_date BETWEEN '2005' AND '2021' " +
      "GROUP BY year " +
      "ORDER BY year")//.show()

    q3.coalesce(1).write.format("com.databricks.spark.csv")
      .save("output/question12/yearly_rsvp")

    //-----------------------------------------------------------------------------------------------------------------
    // Sub-question: Find total rsvp_limit for each month in 2020.
    val q4a = spark.sql("SELECT SUBSTRING(view1.local_date, 6, 2) AS months, " +
      "ROUND(COUNT(view1.rsvp_limit), 2) AS total_rsvp_limit_2020 " +
      "FROM view1 " +
      "WHERE view1.local_date LIKE '2020%' " +
      "GROUP BY months " +
      "ORDER BY months")//.show()

    // Sub-question: Find total rsvp_limit for each month in 2019.
    val q4b = spark.sql("SELECT SUBSTRING(view1.local_date, 6, 2) AS months, " +
      "ROUND(COUNT(view1.rsvp_limit), 2) AS total_rsvp_limit_2019 " +
      "FROM view1 " +
      "WHERE view1.local_date LIKE '2019%' " +
      "GROUP BY months " +
      "ORDER BY months")//.show()

    // Sub-question: Find total rsvp_limit for each month in 2018.
    val q4c = spark.sql("SELECT SUBSTRING(view1.local_date, 6, 2) AS months, " +
      "ROUND(COUNT(view1.rsvp_limit), 2) AS total_rsvp_limit_2018 " +
      "FROM view1 " +
      "WHERE view1.local_date LIKE '2018%' " +
      "GROUP BY months " +
      "ORDER BY months")//.show()

    // Total rsvp_limit comparisons between 2020, 2019, and 2018.
    val q4 = q4a.join(q4b.join(q4c, "months"), "months").orderBy($"months".asc)

    q4.coalesce(1).write.format("com.databricks.spark.csv")
      .save("output/question12/monthly_rsvp_2018to2020")

    //-----------------------------------------------------------------------------------------------------------------
    // Plot stacked bar chart
    plotStackedBar(q2, "Tech Events Created Monthly From 2018-2020", "Q6Monthly", "question6")
    plotStackedBar(q4, "Total RSVP Limit Monthly From 2018-2020", "Q12Monthly", "question12")

    // Plot bar charts
    plotBar(q1, "Tech Events Created Yearly From 2002-2020", "Q6Yearly", "question6")
    plotBar(q3, "Total RSVP Limit Yearly From 2005-2020", "Q12Yearly", "question12")
  }

  /**
   * Plot stacked bar chart
   * @param df Dataframe results for plotting where first column is the x-axis labels.
   * @param title Name of the chart.
   * @param fname File name of the png(chart image) saved under a directly path.
   */
  def plotStackedBar(df: DataFrame, title: String, fname: String, path: String): Unit ={
    // Convert df into Array of Rows
    val arr = df.collect()

    var seq = Seq[Double]()
    var yAxis = Seq[Seq[Double]]()
    var xAxis = Seq[String]()

    // Populate x-axis
    arr.foreach(row => for(i <- 0 until row.length){
      if(i == 0){
        xAxis = xAxis :+ row(i).toString
      }
    })
    // Populate stacked y-axis
    arr.foreach(row => for(i <- 1 until row.length){
      seq = seq :+ row(i).toString.toDouble
      if(i == row.length-1){
        yAxis = yAxis :+ seq
        seq = Seq[Double]()
      }
    })

    val colors = Color.stream.take(1)
    // Plot stacked bar chart with three different years
    BarChart.stacked(yAxis, labels = Seq("2020", "2019", "2018")) // Name your stacked labels
      .title(title)
      .xAxis(xAxis)
      .yAxis()
      .ybounds(0) // Set y-axis to start at 0
      .frame()
      .bottomLegend()
      .render()
      .write(new File(s"out/${path}/${fname}.png"))

  }

  /**
   * Plot normal bar chart
   * @param df Takes in dataframe results from Spark SQL.
   * @param title Name of the chart.
   * @param fname File name of the png(chart image) saved under a directly path.
   */
  def plotBar(df: DataFrame, title: String, fname: String, path: String): Unit ={
    // Convert dataframe into sequence of doubles for axes
    val seq = df.collect().map(row => row.toSeq.map(_.toString.toDouble)).toSeq.flatten

    var yAxis = Seq[Double]()
    var xAxis = Seq[String]()

    // Populate x-axis and y-axis
    for(i <- 0 until seq.length){
      if(i%2 == 0){
        xAxis = xAxis :+ (seq(i).toInt.toString)
      }
    }
    for(i <- 0 until seq.length){
      if(i%2 != 0){
        yAxis = yAxis :+ seq(i)
      }
    }
    // Plot a bar chart
    BarChart.custom(yAxis.map(Bar.apply))
      .title(title)
      .standard(xLabels = xAxis)
      .ybounds(lower = 0)
      //.hline(0)
      //.xAxis(labels)
      //.yAxis()
      //.frame()
      .render()
      .write(new File(s"output/${path}/${fname}.png"))
  }
}
