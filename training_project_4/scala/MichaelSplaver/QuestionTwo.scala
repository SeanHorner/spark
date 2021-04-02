package MichaelSplaver

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object QuestionTwo {
  def questionTwo(spark: SparkSession): Unit = {

    import spark.implicits._

    //read in all events data
    val df = spark.read.format("com.databricks.spark.csv")
      .option("header", true)
      .option("delimiter", "\t").csv("input/all_cities.tsv")

    val cat_name = udf[String, Int](Categories.getCategory)

    //get categories
    val catsDf = df
      .withColumn("cat_ids",split($"cat_ids",", "))
      .filter(not(array_contains($"cat_ids","34")))
      .select(explode($"cat_ids").as("cat_id"))
      .withColumn( "cat_name", cat_name($"cat_id"))
      .groupBy($"cat_name")
      .count()
      .orderBy(desc("count"))

    //save to file
    catsDf
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .save("output/question2/categoriesCount")

    //write and save to graph
    Plotting.plotBar(catsDf.limit(5),"Popular Categories Alongside Tech","output/question2/popularCatsTop5.png")
  }
}
