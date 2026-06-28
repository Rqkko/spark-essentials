package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {
  val spark = SparkSession.builder()
    .appName("DF Columns and Expression")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)

  // various select methods
  import spark.implicits
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    expr("Origin") // EXPRESSION
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  // selectExpr
  var carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing

  // adding a column
  var carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")

  // remove col
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // Filtering
  val eurepeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  val americanCarsDFWhere = carsDF.where(col("Origin") === "USA")
  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter((col("Origin") === "USA").and(col("Horsepower") > 150))
  val americanPowerfulCarsDF2Infix = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150) // Infix (like natural lang)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if DFs have same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()

  /**
   * Exercises
   * 1. Read the movies DF and select 2 columns of your choice
   *
   * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
   *
   * 3. Select all COMEDY movies (Major_Genre) with IMDB rating above 6
   *
   * Use as many versions as possible
   */

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  moviesDF.show()

  // Exercise 1
  val moviesTwoColsDF = moviesDF.select(
    moviesDF.col("Title"),
    expr("IMDB_Rating")
  )
  moviesTwoColsDF.show()

  // Exercise 2
  val moviesDFWithTotalProfit = moviesDF.withColumn("Total_Profit", col("US_Gross") + col("Worldwide_Gross"))
  moviesDFWithTotalProfit.show()

  val moviesProfitDF = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Profit"),
    expr("US_Gross + Worldwide_Gross + US_DVD_Sales")
  )
  moviesProfitDF.show()

  // Exercise 3
  val comedyMoviesDF =  moviesDF
    .select(
      "Title",
      "Major_Genre",
      "IMDB_Rating"
    ).where(
      col("Major_Genre") === "Comedy" and
      col("IMDB_Rating") > 6
    )
  comedyMoviesDF.show()
}
