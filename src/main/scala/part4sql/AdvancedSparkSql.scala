package part4sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AdvancedSparkSql {
  val spark = SparkSession.builder()
    .appName("Advanced Spark SQL")
    .master("local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read.json("src/main/resources/data/cars.json")
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  def main(args: Array[String]): Unit = {
    carsDF.createOrReplaceTempView("cars")
    moviesDF.createOrReplaceTempView("movies")

    /*
      > spark 4.0 - ANSI mode
      - division by zero -> throws errors
      - invalid casts -> throw errors
      - arithmetic overflow -> throws errors
     */

    // pipe syntax > 4.0
    val moviesProcessedDF = moviesDF
      .where(col("IMDB_Rating") > 8.0)
      .select("Title", "IMDB_Rating")
      .orderBy(col("IMDB_Rating").desc)
      .limit(10)

    // '|>' is the pipe operator
    // e.g. output from 'SELECT * FROM movies' become the input for 'WHERE IMDB_Rating > 8.0'
    val moviesProcessedDF_v2 = spark.sql(
      """
        |SELECT * FROM movies
        ||> WHERE IMDB_Rating > 8.0
        ||> SELECT Title, IMDB_Rating
        ||> ORDER BY IMDB_Rating DESC
        ||> LIMIT 10
        |""".stripMargin
    )

    // Scala UDFs
    val extractBrandFn = (name: String) =>
      name.split(" ")(0).capitalize

    val extractBrandUDF = udf(extractBrandFn)
    spark.udf.register("nameUDF", extractBrandUDF)

    val carBrandsDF = spark.sql(
      """
        |SELECT nameUDF(Name) from cars
        |""".stripMargin
    )

    carBrandsDF.show()

    // SQL UDFs - may be more performant
    // 1 - create function
    spark.sql("CREATE FUNCTION lbs_to_kg(lbs DOUBLE) RETURNS DOUBLE RETURN lbs / 2.2")

    // 2 - use it under registered name
    val carWeightsDF = spark.sql(
      """
        |SELECT Name, lbs_to_kg(Weight_in_lbs) FROM cars
        |""".stripMargin
    )

    carWeightsDF.show()

    // session variables
    spark.sql("DECLARE min_rating = 7.0")

    val maybeGoodMoviesDF = spark.sql(
      """
        |SELECT Title, IMDB_Rating FROM movies
        |WHERE IMDB_Rating > min_rating
        |ORDER BY IMDB_Rating DESC
        |""".stripMargin
    )

    // change var
    spark.sql("SET VARIABLE min_rating = 8.0")
    // can run query again ...

    // recursion
    // generate numbers 1 to 10
    def generateNumbers(n: Int): List[Int] = {
      def aux(i: Int): List[Int] =
        if (i >= n) List()
        else i :: aux(i + 1)

      aux(1)
    }

    val numbersDF = spark.sql(
      """
        |WITH RECURSIVE numbers AS(
        | SELECT 1 as n
        | UNION ALL
        | SELECT n + 1 FROM numbers WHERE n < 10
        |)
        |""".stripMargin
    )

    val myPreferredDatesDF = spark.sql(
      """
        |WITH RECURSIVE date_range AS (
        | SELECT DATE '2026-05-13' as dt
        | UNION ALL
        | SELECT dt + INTERVAL 1 DAY FROM date_range WHERE dt < DATE '2026-06-02'
        |)
        |SELECT * FROM date_range
        |""".stripMargin
    )

    val fiboDF = spark.sql(
      """
        |WITH RECURSIVE fibonacci AS (
        | SELECT 1 as n, CAST(0 AS BIGINT) AS a, CAST(1 AS BIGINT) AS b
        | UNION ALL
        | SELECT n + 1, b, a + b FROM fibonacci WHERE n < 15
        |)
        |SELECT * FROM fibonacci
        |""".stripMargin
    )
  }
}
