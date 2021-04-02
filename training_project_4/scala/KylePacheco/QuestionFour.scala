package KylePacheco

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object QuestionFour {

  def mainRunner() = {

    val dataFile = "input/all_cities_array.json" //Change this to proper path for data
    val savePathQuestionFour = "output/question4/" //Change this to desired path when saving file

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("AnalysisTesting")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val rawEventDF = spark.read
      .option("multiline","true").json(dataFile)


      //Use the code below if you're using a tsv file
      //.option("sep","\t").option("header","true").csv(tsvFile)

//    rawEventDF.show(20)

    val eventDF = rawEventDF
      .withColumn("yes_rsvp_count",col("yes_rsvp_count").cast(IntegerType))
    //Getting the highest RSVP count
    //eventDF.select("name","yes_rsvp_count","is_online_event")

    val onlineDF = eventDF.select("name","yes_rsvp_count","is_online_event")
      .filter($"is_online_event"==="true")
      .sort($"yes_rsvp_count".desc)
      .limit(100)
      .write
      .option("header","true")
      .mode("overwrite")
      .format("csv")
      .save(savePathQuestionFour+"Online")

    val offlineDF = eventDF.select("name","yes_rsvp_count","is_online_event")
      .filter($"is_online_event"==="false")
      .sort($"yes_rsvp_count".desc)
      .limit(100)
      .write
      .option("header","true")
      .mode("overwrite")
      .format("csv")
      .save(savePathQuestionFour+"InPerson")


    spark.stop()
  }


}
