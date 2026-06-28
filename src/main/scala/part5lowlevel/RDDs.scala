package part5lowlevel

import org.apache.spark.sql.functions.{coalesce, col, expr, lit, try_variant_get}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object RDDs extends App {
  val spark = SparkSession.builder()
    .appName("Intro to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  /**
   * Ways to create RDDs
   */
  // 1 - parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - reading the files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0)) // In this case, the header is the only one in lowercase
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from DF
  import spark.implicits._
  val stocksDF = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/data/stocks.csv")
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers") // you lose type info

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD) // you keep type info

  // Transformations

  // distinct
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
  val msCount = msftRDD.count() // eager action

  // counting
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // also lazy

  // min and max
  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan[StockValue]((sa: StockValue, sb: StockValue) => sa.price < sb.price)
  val minMsft = msftRDD.min() // action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  // ^^ very expensive (shuffling) ^^

  // Partitioning
  /*
    Repartitioning is expensive. Involves Shuffling
    Best practice: partition early, then process that
      - Size of a partition should be 10-100MB
   */
  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")


  // coalesce - reduce the no. of partition
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // does not involve shuffling
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

  /**
   * Exercises
   */
  // 1. Read the movies.json as an RDD
  case class Movie(title: String, genre: String, rating: Double)

  val moviesDF = spark.read.option("header", "true").option("inferSchema", "true").json("src/main/resources/data/movies.json")
  val columnTruncatedMoviesDF = moviesDF.select(
    col("Title").as("title"),
    col("Major_Genre").as("genre"),
    coalesce(col("IMDB_Rating"), lit(0)).as("rating")
  )
  val moviesDS = columnTruncatedMoviesDF.as[Movie]
  val moviesRDD = moviesDS.rdd
//  moviesRDD.collect().foreach(println)
  moviesRDD.toDF.show()

  // 2. Show the distinct genres as an RDD
  val genresRDD = moviesRDD.map(_.genre).distinct()

  // 3. Select all the movies in the Drama genre with IMDB rating > 6
  val goodDramsRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)

  // 4. Show the avg rating of movies by genre
  case class GenreAvgRating(genre: String, rating: Double)
  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }
//  val avgRat = moviesRDD.groupBy(_.genre)
//  avgRat.foreach(println)
}
