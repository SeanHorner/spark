package MichaelSplaver.data_collection

import java.io.File

import MichaelSplaver.{Categories, Locations}
import Shared.description_scrubber
import org.apache.spark.sql.{SparkSession, functions}
import scalaj.http.{Http, HttpOptions}
import org.apache.spark.sql.functions.{array, concat_ws, transform, udf}
import org.apache.spark.sql.types.{ArrayType, LongType, StringType}

object Runner {
  def mainRunner(): Unit = {
    val authCode = JsonHelper.getValue("credentials.json", "auth-code")
    requestCategoriesToFile(authCode)

    val spark = SparkSession.builder()
      .appName("BD-Project")
      .master("local[8]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._


    val cat_name = udf[String, Int](Categories.getCategory)

    spark.read.json("output/categories.json")
      .select($"category_ids", $"name")
      .show(100)

    spark.read.json("all-events-distinct.json/all_cities.json")
      .printSchema()

    Locations.cityLocationsList_1.foreach((city: String) => {
      val cityNameRegex = ".+?(?=,)".r
      val cityName = cityNameRegex.findFirstIn(city).get
      val groupsInCity = requestGroups(authCode,city)
      val millis = System.currentTimeMillis.toString
      JsonHelper.writeToFile(s"output/groups/$cityName-$millis.json", groupsInCity)
      val groupUrls = groupUrlFromGroups(spark, s"output/groups/$cityName-$millis.json")
      groupUrls.foreach((groupUrl: String) => {
        requestEventsToFile(authCode,groupUrl)
      })
    })

    spark.read.json("output/groups")
      .distinct()
      .select($"name", $"status", $"members", $"urlname")
      //.withColumn("catergory_ids", array($"group.meta_category.category_ids"))
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", true)
      .option("delimiter", "\t")
      .save("output/all-groupsV2.tsv")

    val scrub = udf[String, String](description_scrubber.description_scrubber)
    val month = udf[String, String](date_to_month)

    spark.read.json("output/all-events-distinct.json/all_cities.json")
      .distinct()
      .select($"id", $"name", $"group.name" as "group_name", $"group.urlname", $"venue.id" as "v_id",
        $"venue.name" as "v_name", $"local_date", month($"local_date".cast(StringType)) as "date_month", $"local_time",
        $"group.localized_location", $"is_online_event", $"status", concat_ws(", ",$"group.meta_category.category_ids") as "cat_ids",
        $"duration", $"time", $"created", $"yes_rsvp_count", $"rsvp_limit", $"fee.accepts", $"fee.amount",
        scrub($"description") as "description")
      //.withColumn("catergory_ids", array($"group.meta_category.category_ids"))
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", true)
      .option("delimiter", "\t")
      .save("output/all-events-distinct.tsv")

    spark.read.json("output/all-events-distinct.json/all_cities.json")
      .select($"group.meta_category.category_ids", $"group.meta_category.name", $"id", $"name", $"group.name")
      .filter(functions.size($"category_ids") > 1)
      .show(false)

    spark.read.json("output/events")
      .distinct()
      .coalesce(1)
      .write.json("output/all-events-distinct.json")
    spark.read.json("output/events").coalesce(1).write.json("output/all-events.json")

    requestEventsToFile(authCode, "NYC-Women-in-Data-Science-Big-Data-ML-Blockchain-Web-Dev")
  }

  def date_to_month(date: String): String = {
    var short_date = date + " "
    if (short_date.matches("\\d+-\\d+-\\d+ ")) {
      short_date = date.replaceFirst("(\\d+-\\d+)-\\d+", "$1")
    }
    short_date
  }

  def saveJsonCatsToFile(catergories: String) : Unit = {
    val millis = System.currentTimeMillis.toString
    JsonHelper.writeToFile(s"output/categoriescategories-$millis.json", catergories)
  }

  def saveJsonEventsToFile(groupUrl: String, events: String, chainNum: Int) : Unit = {
    val millis = System.currentTimeMillis.toString
    JsonHelper.writeToFile(s"output/events/$groupUrl-events-$chainNum-$millis.json", events)
  }

  def getListOfFiles(directory: String) : List[File] = {
    new File(directory).listFiles.toList
  }

  def requestCategoriesToFile(accessToken: String): Unit = {
    val result = Http(s"https://api.meetup.com/find/topic_categories")
      .header("Authorization", s"Bearer $accessToken")
      .option(HttpOptions.readTimeout(60000)).asString

    saveJsonCatsToFile(result.body)
  }

  def requestEventsToFile(accessToken: String, urlname: String): Unit = {
    val result = Http(s"https://api.meetup.com/$urlname/events")
      .param("status", "past,cancelled")
      .param("fields", "meta_category,series")
      .header("Authorization", s"Bearer $accessToken")
      .option(HttpOptions.readTimeout(60000)).asString

    println(s"Request made to https://api.meetup.com/$urlname/events")

    val requestsRemaining = result.headers("X-RateLimit-Remaining").head.toInt
    Console.println(s"Remaining Requests: $requestsRemaining")
    if (requestsRemaining <= 0) {
      Console.println(s"No Remaining Requests Sleeping Thread for 10 seconds...")
      Thread.sleep(10000)
    }

    saveJsonEventsToFile(urlname, result.body,1)

    if (result.headers.contains("Link")) {
      val next = "(?<=<).*?(?=>; rel=\"next\")".r.findFirstIn(result.headers("Link").head)
      if (next.isDefined) {
        requestEventToFileChained(accessToken, urlname, next.get, 2)
      }
    }
  }

  def requestEventToFileChained(accessToken: String, urlName: String, next: String, chainNum: Int): Unit = {
    val result = Http(next)
      .header("Authorization", s"Bearer $accessToken")
      .option(HttpOptions.readTimeout(60000)).asString

    val requestsRemaining = result.headers("X-RateLimit-Remaining").head.toInt
    Console.println(s"Remaining Requests: $requestsRemaining")
    if (requestsRemaining <= 0) {
      Console.println(s"No Remaining Requests Sleeping Thread for 10 seconds...")
      Thread.sleep(10000)
    }

    saveJsonEventsToFile(urlName, result.body,chainNum)

    if (result.headers.contains("Link")) {
      val next = "(?<=<).*?(?=>; rel=\"next\")".r.findFirstIn(result.headers("Link").head)
      if (next.isDefined) {
        println(s"Next Chain Link: ${next.get}")

        if(("(?<=scroll=since%3A)[^-]*".r.findFirstIn(next.get).get.toInt) <= 2021) {
          requestEventToFileChained(accessToken, urlName, next.get, chainNum+1)
        }
        else{
          println(s"TIME TRAVELERS SPOTTED at ${next.get}")
        }
      }
    }
  }

  def requestGroups(accessToken: String, city: String): String = {
    val result = Http(s"https://api.meetup.com/find/groups")
      .params(Seq(("location", city),("category","34"),("order","members")))
      .header("Authorization", s"Bearer $accessToken")
      .option(HttpOptions.readTimeout(60000)).asString

    println(s"Request made to https://api.meetup.com/find/groups for CITY: $city")

    val requestsRemaining = result.headers("X-RateLimit-Remaining").head.toInt
    Console.println(s"Remaining Requests: $requestsRemaining")
    if (requestsRemaining <= 0) {
      Console.println(s"No Remaining Requests Sleeping Thread for 10 seconds...")
      Thread.sleep(10000)
    }
    result.body
  }

  def groupUrlFromGroups(spark: SparkSession, filePath: String): List[String] = {
    import spark.implicits._

    val origDF = spark.read.json(filePath)
    if (origDF.isEmpty) List()
    else origDF.select($"urlname").coalesce(1).collect().map(row => row(0).toString).toList
  }
}

