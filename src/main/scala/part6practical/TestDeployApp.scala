package part6practical

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TestDeployApp {
  val spark = SparkSession.builder()
    .appName("Test Deploy App")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val moviesDF = spark.read.json(args(0))

    val goodComediesDF = moviesDF
      .filter(col("Major_Genre") === "Comedy" and  col("IMDB_Rating") > 6.5)
      .select(
        col("Title"),
        col("IMDB_Rating").as("Rating"),
        col("Release_Date")
      )
      .orderBy(col("Rating").desc_nulls_last)

    goodComediesDF.show()

    goodComediesDF.write.mode(SaveMode.Overwrite).json(args(1))
  }
}
