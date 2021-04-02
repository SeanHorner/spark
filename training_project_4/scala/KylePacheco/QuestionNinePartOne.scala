package KylePacheco

import org.apache.spark.sql._
import org.apache.log4j._
import java.io._
import org.apache.spark.sql.functions._

object QuestionNinePartOne {
//  How many upcoming events are online compared to in person ones (and what cities have the most upcoming in person events)?

  def mainRunner(): Unit = {

    val dataFile = "all_cities_array.json"
//    val savePathQuestionNine = "C:\\Users\\kylep\\Desktop\\StagingProject\\Code\\Q9P1"
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
      .select(concat($"city",lit(", "),$"state").as("city/state"))
      .count()

    val onlineDFCount = eventDF.filter($"is_online_event"==="true")
//      .select("venue","is_online_event")
//      .withColumn("city",$"venue.city")
//      .withColumn("state",$"venue.state")
//      .filter($"city"=!="null")
//      .filter($"state"=!="null")
//      .select(concat($"city",lit(", "),$"state").as("city/state"))
      .count()

    spark.stop()

    val writer = new PrintWriter(new File(savePathQuestionNine + "Q9P1.txt" ))
    writer.write(s"Number of in person events: $offlineDFCount\n")
    writer.write(s"Number of online events: $onlineDFCount")
    writer.close()

  }

}
