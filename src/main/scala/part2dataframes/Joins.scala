package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}

object Joins extends App {
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitars.json")
  val guitaristsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitarPlayers.json")
  val bandsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/bands.json")

  // inner joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  // outer joins
  // left outer = everything in the inner join = left table
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  // right outer
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // outer join = inner join + both tables
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-joins
  // left semi = left columns + right key, the rest is gone
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti-joins = left columns + right key not satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show()

  // things to bear in mind
  //  guitaristsDF.select("id", "band").show // this crashes (ambiguous column name)

  // option 1 - rename column which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop dupe column
  guitaristsBandsDF.drop(bandsDF.col("id")) // spark keep id of each column, so not confused when using 'guitaristsBandsDF' & 'bandsDF'

  // option 3 - rename the offending column and keep data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  // using complex types as join cond
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

  /**
   * Exercises
   */



  // 1. show all employees and their max salary
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  val salariesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.salaries")
    .load()

  // Actually should find max salary first then join
  // can do salariesDF.groupBy("emp_no").max("salary")
  val empSalJC = employeesDF.col("emp_no") === salariesDF.col("emp_no")
  val empSalDF = employeesDF.join(salariesDF, empSalJC, "left_outer")
  empSalDF.groupBy(employeesDF.col("emp_no"), employeesDF.col("first_name"))
    .agg(
      max("salary")
    ).show


  // 2. show all employees who were never managers
  val deptManagersDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.dept_manager")
    .load()

  val empDepJC = employeesDF.col("emp_no") === deptManagersDF.col("emp_no")
  val nonMngrEmpDF = employeesDF.join(deptManagersDF, empDepJC, "left_anti")
  nonMngrEmpDF.show

  // 3. find the job titles of the best paid 10 employees in the company
  val titlesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.titles")
    .load()

  val salTitleJC = salariesDF.col("emp_no") === titlesDF.col("emp_no")
  val salTitleDF = salariesDF.join(titlesDF, salTitleJC)
  salTitleDF.orderBy(col("salary").desc).limit(10).show
}
