package part3typesdatasets

import org.apache.spark.sql.functions.{array_contains, avg, col, to_date}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import java.time.LocalDate
import scala.math.Ordered.orderingToOrdered

object Datasets extends App {
  val spark = SparkSession.builder()
    .appName("Datasets")
    .master("local")
    .getOrCreate()

  val numbersDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  // convert DF -> DS
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  numbersDS.filter(_ < 100)

  // dataset of a complex type
  // 1 -define complex type
  case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double],
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: LocalDate, // LocalDate is the new version's approach
                Origin: String
                )

  // 2 - read DF from file
  def readDF(filename: String) = spark.read.json(s"src/main/resources/data/$filename")

  // 3 - define encoder (importing implicits)
  import spark.implicits._ // import encoders you will ever use
  val carsDF = readDF("cars.json")
  val typedCarsDF = carsDF.withColumn("Year", to_date($"Year", "yyyy-MM-dd"))
  typedCarsDF.show()
  // 4 - convert DF to DS
  val carsDS = typedCarsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100).show()

  // map flatMap, fold, reduce, for comprehensions
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())

  carNamesDS.show()

  /**
   * Exercise
   * 1. Count how many cars we have
   * 2. Count how many powerful cars we have (HP > 140)
   * 3. Compute Avg HP for entire dataset
   */
  // 1
  println(carsDS.count())
  // 2
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count())
  // 3
  val carsCount = carsDS.count
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)

  // can also use DF functions
  carsDS.select(avg(col("Horsepower"))).show

  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id :Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")
  guitarPlayerBandsDS.show

  /**
   * Exercise: join the guitarsDS and guitarPlayersDS
   * (hint: use array_contain)
   */

  val guitarGuitarPlayersDS = guitarPlayersDS.joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer").show()

  // Grouping DS
  val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()
    .show()

  // joins and groups are WIDE transformations, will involve SHUFFLE operation
}
