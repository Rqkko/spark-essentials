package part7bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.setActiveSession

object TaxiApplication extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Taxi Big Data Application")
    .getOrCreate()

  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiDF.printSchema()
  println(taxiDF.count())

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")
  taxiZonesDF.printSchema()
}
