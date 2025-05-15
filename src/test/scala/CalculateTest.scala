import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

class CalculateTest extends AnyFunSuite {

  //helps hide other spark logs except result
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)

  // Create a Spark session for testing
  val spark: SparkSession = SparkSession.builder()
    .appName("CalculateTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // sample data
  val flights: Dataset[Flight] = Seq(
    Flight(1, 101, "UK", "FR", "2023-01-10"),
    Flight(1, 102, "FR", "US", "2023-01-20"),
    Flight(2, 103, "UK", "DE", "2023-02-10"),
    Flight(3, 104, "US", "CN", "2023-02-15"),
    Flight(3, 105, "CN", "UK", "2023-02-28")
  ).toDS()

  val passengers: Dataset[Passenger] = Seq(
    Passenger(1, "Alice", "Smith"),
    Passenger(2, "Bob", "Johnson"),
    Passenger(3, "Carol", "Davis")
  ).toDS()

  test("question1 should count flights by month") {
    println("Output for question 1:")
    val result = flights
      .withColumn("Month", month(to_date($"date", "yyyy-MM-dd")))
      .groupBy("Month")
      .count()
      .withColumnRenamed("count", "Number of Flights")
      .orderBy("Month")

    result.show() // Print to console
  }

  test("question2 should list top 100 flyers") {
    println("Output for question 2:")
    val result = flights
      .groupBy("passengerId")
      .count()
      .withColumnRenamed("count", "Number of Flights")
      .orderBy(desc("Number of Flights"))
      .limit(100)
      .join(passengers, "passengerId")
      .select(
        col("passengerId"),
        col("Number of Flights"),
        col("firstName").as("First name"),
        col("lastName").as("Last name")
      )

    result.show() // Print to console
  }


}
