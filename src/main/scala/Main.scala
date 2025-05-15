import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DatasetHolder
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import java.sql.Date
import java.time.LocalDate

//store modals/input
//has useful methods,immutable by default


//output



//not necessary since cc
//object CustomImplicits {
//  implicit val PassengerEncoder: Encoder[Passenger] = Encoders.product[Passenger]
//  implicit val FlightEncoder: Encoder[Flight] = Encoders.product[Flight]
//}

object Main {
  def main(args: Array[String]): Unit = {
    //println("Hello world!")

    //helps hide other spark logs except result
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    //instantiate spark session
    val spark = SparkSession.builder()
      .appName("Flight Data Analysis")
      .master("local[*]") //run in local mode
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
      .getOrCreate() //return existing or create new one -> prevent multiple sessions

    //handle encoder for common data type in case class
    import spark.implicits._
    //read csv to datasets
    val passenger_ds = spark.read.option("header", true).option("inferSchema", true)//1st row contain header,infer data types from data
      .csv("data/passengers.csv")
      .as[Passenger]

    val flight_ds = spark.read.option("header", true).option("inferSchema", true)
      .csv("data/flightData.csv")
      .as[Flight]//case class input

    val answer1 = Calculate.question1(flight_ds)(spark)
    val answer2 = Calculate.question2(flight_ds,passenger_ds)(spark)
    val answer3 = Calculate.question3(flight_ds)(spark)
    val answer4 = Calculate.question4(flight_ds)(spark)

    val fromDate = Date.valueOf(LocalDate.of(2017, 1, 1))
    val toDate = Date.valueOf(LocalDate.of(2017, 12, 31))
    val answer5 = Calculate.question5(flight_ds, 5, fromDate, toDate)(spark)

    answer1.show(10)
    answer2.show(10)
    answer3.show(10)
    answer4.show(10)
    answer5.show(10)

    //write to csv
/*    answer1.write.option("header", true).mode("overwrite").csv("C:/Users/telet/Documents/question1")
    answer2.write.option("header", true).mode("overwrite").csv("C:/Users/telet/Documents/question1")
    answer3.write.option("header", true).mode("overwrite").csv("C:/Users/telet/Documents/question1")
    answer4.write.option("header", true).mode("overwrite").csv("C:/Users/telet/Documents/question1")*/

    // After processing the result, coalesce into a single partition
    //savemode append used for getting incremental data in realtime scenario
    //coalesce(1) to ensure Spark writes a single file for each result
    //Spark does not provide a built-in method to directly set the output CSV filename l
    answer1.coalesce(1).write.option("header", true).mode("overwrite").csv("results/question1")
    answer2.coalesce(1).write.option("header", true).mode("overwrite").csv("results/question2")
    answer3.coalesce(1).write.option("header", true).mode("overwrite").csv("results/question3")
    answer4.coalesce(1).write.option("header", true).mode("overwrite").csv("results/question4")
    answer5.coalesce(1).write.option("header", true).mode("overwrite").csv("results/question5")


    spark.stop()
  }
}
