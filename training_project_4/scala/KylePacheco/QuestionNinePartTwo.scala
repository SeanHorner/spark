package KylePacheco

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

object QuestionNinePartTwo {

  def mainRunner(): Unit = {

    val dataFile = "all_cities_array.json"
    val savePathQuestionNine = "output/question9/"

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("AnalysisTesting")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val eventDF = spark.read
      .option("multiline","true").json(dataFile)
      //.select("name","status")
      .filter(col("status")=!="cancelled")

    val offlineDFCount = eventDF.filter($"is_online_event"==="false")
      .select("venue","is_online_event")
      .withColumn("city",$"venue.city")
      .withColumn("state",$"venue.state")
      .filter($"city"=!="null")
      .filter($"state"=!="null")
      .select(upper(concat($"city",lit(", "),$"state")).as("city/state"))
      .groupBy("city/state")
      .count()
      .sort($"count".desc)
      .coalesce(1)
      .write
      .option("header","true")
      .mode("overwrite")
      .format("csv")
      .save(savePathQuestionNine + "Q9P2.csv")


    spark.stop()
  }
}
