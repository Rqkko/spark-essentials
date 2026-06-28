package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// User Defined Functions
object UDFs {
  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // 1 - create scala funciton
  val countWords = (text: String) =>
    text.split("\\s").length // split by space

  // 2 - register the UDF
  val countWordsUDF = udf(countWords)

  // 3 - use the UDF as a normal spark function
  val moviesWithWordCountDF = moviesDF.select(
    col("Title"),
    countWordsUDF(col("Title")).as("Title_Words")
  )

  // UDFs with multiple arguments
  val carsDF = spark.read.json("src/main/resources/data/cars.json")

  // 1
  val carCategoryFn = (weight: Long, cylinders: Long) =>
    if (weight >= 4000 || cylinders >= 8) "Heavy"
    else if (weight >= 3000 || cylinders >= 6) "Medium"
    else "Light"

  // 2
  val carCategoryUDF = udf(carCategoryFn)

  // 3
  val carsWithCategoryDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    col("Cylinders"),
    carCategoryUDF(col("Weight_in_lbs"), col("Cylinders")).as("Category")
  )

  /**
   * Exercise - register UDF that takes the brand of the cars (first word of name), capatalize it,
   * then show all distinct brands ordered alphabetically
   * @param args
   */

  val brandFn = (title: String) =>
    title.split(" ")(0).capitalize

  val brandUDF = udf(brandFn)

  val carBrandsDF = carsDF.select(
    brandUDF(col("Name")).as("Brand")
  ).distinct()
    .orderBy("Brand")

  def main(args: Array[String]): Unit = {
    carBrandsDF.show
  }
}
