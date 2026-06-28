package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {
  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .master("local")
    .getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // Dates
  val moviesWithReleaseDates = moviesDF
    .select(col("Title"), try_to_date(regexp_replace(col("Release_Date"), "-(\\d{2})$", "-19$1"), "d-MMM-yyyy").as("Actual_Release")) // conversion

  moviesWithReleaseDates
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // date_add, date_sub

  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull)

  /**
   * Exercise
   * 1. How to deal with multiple date formats
   * 2. Read the stocks DF and parse the dates
   */

  // 1 - parse DF multiple times, then union the small DFs

  // 2
  val stocksDF = spark.read.format("csv").option("header", "true").load("src/main/resources/data/stocks.csv")

  val stocksDFWithDates = stocksDF
    .withColumn("actual_date", to_date(col("date"), "MMM d yyyy"))

  // Structures

  // 1 - with col operators
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))

  // 2 - with expression strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays
  val moviesWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // return array of strings

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  ).show()
}
