package EdwardReed

import java.io.File

import com.cibo.evilplot.plot.{Bar, BarChart}
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{asc, concat, count, desc, explode, length, lit, max, round, second}

object testQueries {
  def mainRunner(): Unit = {
    val appName = "reader"
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val output15: String = "output/question14/"
    val output8: String = "output/question14/"


    //This dataframe is created using the tsv file
    //val events = spark.read.option("sep", "\t").option("header", "true").csv(args(0))
    //  .withColumn("rsvp_count", $"yes_rsvp_count".cast("Int"))

    val events = spark.read.parquet("input/all_cities.parquet")
      .withColumn("rsvp_count", $"yes_rsvp_count".cast("Int"))
      .withColumn("Venue State", $"localized_location".substr(length($"localized_location") - 2, length($"localized_location") - 1))

    //This dataframe just has the city (localized_location) and a column containing how many events were
    //held in the city.
    val cities = events.select($"localized_location".as("city"))
      .groupBy("city")
      .agg(count("city").as("number of events"))
      .sort(functions.desc("number of events"))

    //cities.show(false)

    //Writes the answer to question 8 to file.
    cities.coalesce(1).write.option("header", "true").option("delimiter", "\t").csv(output8 + "q8.tsv")


    //Creates the dataframe for question 15: creates a dataframe with the event name and the venue's city, date,
    //city and amount of rsvps received
    val venues = events.select($"name".as("Event Name"), $"v_name".as("Venue Name"),
      $"local_date".as("Date of Event"), $"localized_location".as("City"),
      $"yes_rsvp_count".as("RSVPs received"), $"group_name".as("Group Name"), $"Venue State")

    val distinctVenues = venues.select($"Venue Name", $"Venue State").distinct()

    //Creates a dataframe that will contain the each state and the number of venues in each state
    val numStates = distinctVenues.select($"Venue State")
      .groupBy("Venue State")
      .agg(count("Venue State").as("Number of Venues"))
      .orderBy(functions.desc("Number of Events"))

    numStates.coalesce(1).write.option("header", "true").option("delimiter", "\t").csv(output15 + "venues_per_state.tsv")

    val forGraph = numStates.select($"*").limit(15)

    plot(forGraph, "States with the most venues", output15 + "venues_per_state")

    //Returns a dataframe that contains the venue name, the group that hosted the event's name and the location
    //  ot the group
    val popVenues = venues.select($"Venue Name", $"City")
      .filter($"Venue Name" =!= "Online event")
      .groupBy("Venue Name", "City")
      .agg(count("Venue Name").as("Number of Events"))
      .orderBy(functions.desc("Number of Events"))

    //popVenues.show(false)

    //Writes the answer to question 15 to a file.
    popVenues.coalesce(1).write.option("header", "true").option("delimiter", "\t").csv(output15 + "q15.tsv")

  }

  def plot(df: DataFrame, title: String, fname: String): Unit = {
    // Convert dataframe into sequence of doubles for axes
    val seq = df.collect().map(row => row.toSeq.map(_.toString)).toSeq.flatten

    var counts = Seq[Double]()  // y-axis
    var labels = Seq[String]()  // x-axis

    // Populate x-axis and y-axis
    for(i <- 0 until seq.length){
      if(i%2 == 0){
        labels = labels :+ (seq(i))
      }
    }
    for(i <- 0 until seq.length){
      if(i%2 != 0){
        counts = counts :+ seq(i).toDouble
      }
    }
    // Plot a bar chart
    BarChart.custom(counts.map(Bar.apply), spacing = Some(35))
      .title(title)
      .standard(xLabels = labels)
      .xbounds(lower = 0)
      //.hline(0)
      //.xAxis(labels)
      //.yAxis()
      //.frame()
      .render()
      .write(new File(s"C:/Users/kingr/IdeaProjects/staging-project/${fname}.png"))
  }
}
