package MichaelSplaver

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Question16 {
  def question16(): Unit = {

    val spark = SparkSession.builder()
      .appName("BD-Project-Analysis")
      .master("local[8]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    //read in all groups data
    val df = spark.read.format("com.databricks.spark.csv")
      .option("header", true)
      .option("delimiter", "\t").csv("input/all_groups.tsv")

    //order groups by largest member count desc
    val groupsDf = df
      .dropDuplicates("name")
      .select(substring($"name",0,20).as("name"),$"members")
      .withColumn("members",$"members".cast("Int"))
      .orderBy(desc_nulls_last("members"))

    //write to tsv
    groupsDf
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .save("output/question16/largestGroups")

    //plot and save to .png
    Plotting.plotBar(groupsDf.limit(5),"Largest Tech Groups","output/question16/largestGroupsTop5.png")
  }
}
