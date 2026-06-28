package part2dataframes

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._

object UDAF {
  val spark = SparkSession.builder()
    .master("local")
    .getOrCreate()

  val carsDF = spark.read.json("src/main/resources/data/cars.json")

  // Concatenate all Car names with comma
  // 1 define Scala func
  // need: col type, buffer, final type
  object Concatenator extends Aggregator[String, String, String] {
    override def zero: String = ""

    override def reduce(buffer: String, newValue: String): String = {
      if (buffer.isEmpty) newValue
      else if (newValue.isEmpty) buffer
      else s"$buffer, $newValue"
    }

    override def merge(b1: String, b2: String): String = {
      if (b1.isEmpty) b2
      else if (b2.isEmpty) b1
      else s"$b1, $b2"
    }

    override def finish(finalBuffer: String): String =
      finalBuffer

    override def bufferEncoder: Encoder[String] = Encoders.STRING

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }

  // 2 - register as UDAF
  val concatenatorUDAF = udaf(Concatenator)

  // 3 - apply in dataframe
  val allCarNamesDF = carsDF.select(concatenatorUDAF(col("Name")).as("All_Cars"))


  // CGR (Compound Growth Rate) for values
  // ex. stock prices at 4 time [100, 110, 132, 198]
  // [110/100 = 1.1, 132/110 = 1.2 198/132 = 1.5]
  // (1.1 * 1.2 * 1.5) ^ (1/3) = ...

  // 1
  case class CGRBuffer(product: Double, nIntervals: Long, lastValue: Double)
  object CGR extends Aggregator[Double, CGRBuffer, Double] {
    override def zero: CGRBuffer =
      CGRBuffer(1.0, 0, -1.0)

    override def reduce(b: CGRBuffer, a: Double): CGRBuffer =
      if (b.lastValue < 0) b.copy(lastValue = a)
      else {
        val gr = a / b.lastValue
        CGRBuffer(b.product * gr, b.nIntervals + 1, a)
      }

    override def merge(b1: CGRBuffer, b2: CGRBuffer): CGRBuffer =
      CGRBuffer(b1.product * b2.product, b1.nIntervals + b2.nIntervals, b2.lastValue) // Last value does not really matter since the ratio is already craeted

    override def finish(reduction: CGRBuffer): Double =
      math.pow(reduction.product, 1.0 / reduction.nIntervals)

    override def bufferEncoder: Encoder[CGRBuffer] = Encoders.product // use for case class or tuple

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  // 2
  val cgrUDAF = udaf(CGR)

  // 3
  val msftCmgr = spark.read
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")
    .filter(col("symbol") === "GOOG")
    .agg(cgrUDAF(col("price")))

  def main(args: Array[String]): Unit = {
//    allCarNamesDF.write.json("src/main/resources/data/all_cars.json")

    msftCmgr.show
  }
}
