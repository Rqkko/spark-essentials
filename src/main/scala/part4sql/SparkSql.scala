package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkSql extends App {
  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .master("local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin
  )

  // we can run any sql statement
  spark.sql("create database rtjvm") // create in spark-warehouse directory (by default)
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")
//  databasesDF.show()

  // transfer tables from a DB to spark tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName) // load in memory (so can refer same name in spark sql)
    if (shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager"
  ))

  // read DF from loaded Spark tables
  val employeesDF2 = spark.read.table("employees")

  /**
   * Exercises
   */

  // 1. Read movies DF and store it as Spark table in the rtjvm database.
//  val moviesDF = spark.read
//    .option("inferSchema", "true")
//    .json("src/main/resources/data/movies.json")
//
//  moviesDF.createOrReplaceTempView("movies")
//  moviesDF.write
//    .mode(SaveMode.Overwrite)
//    .saveAsTable("movies")

  // 2. Count how many employees we have in b/w Jan 1 2000 and Jan 1 2001
  spark.sql(
    """
      |select count(1)
      |from employees
      |where hire_date > '2000-01-01' and hire_date < '2001-01-01'
    """.stripMargin
  ).show()


  // 3. Show avg salaries for the employees hired in between those dates, grouped by department.
  spark.sql(
    """
      |select de.dept_no, avg(s.salary)
      |from employess e, dept_emp de, salaries s
      |where
      | e.hire_date > '2000-01-01' and e.hire_date < '2001-01-01' and
      | e.emp_no = de.emp_no and
      | e.emp_no = s.emp_no
      |group by de.dept_no
    """.stripMargin
  )

  // 4. Show the name fo the best-paying department for employees hired in between those dates.
  spark.sql(
    """
      |select avg(s.salary) payments, d.dept_name
      |from employess e, dept_emp de, salaries s, departments d
      |where
      | e.hire_date > '2000-01-01' and e.hire_date < '2001-01-01' and
      | e.emp_no = de.emp_no and
      | e.emp_no = s.emp_no and
      | de.dept_no = d.dept_no
      |group by d.dept_name
      |order by payments desc
      |limit 1
  """.stripMargin
  )
}
