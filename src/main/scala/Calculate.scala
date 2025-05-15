/*
All calculation done here. take case class as input and return as output. write to csv done in main,scala
*/

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import java.sql.Date


//singleton object/single instance of the class
object Calculate {

  //my method to get results
  // total number of flights for each month

  //def question1(flights: Dataset[Flight], outputPath: String): Unit = {
  //passed sparksession var to use spark in the methodd
  def question1(flights: Dataset[Flight])(implicit spark: SparkSession): Dataset[MonthlyFlightStat] = {
    import spark.implicits._
      //val numFlight = flights
    flights
      //get only month,has to parse since its str
      .withColumn("Month", month(to_date(col("date"), "yyyy-MM-dd")))
      .groupBy("Month")
      .count() //returns Long in spart,need to update modals
      .withColumnRenamed("count", "numberOfFlights")
      .select(
        col("Month"),
        col("numberOfFlights"))
      .as[MonthlyFlightStat]
      .orderBy("Month")
    //num_flight.write.option("header", true).mode("overwrite").csv(outputPath)
  }

  //names of the 100 most frequent flyers
  def question2(flights: Dataset[Flight], passengers: Dataset[Passenger])(implicit spark: SparkSession): Dataset[TopFlyer] = {
    import spark.implicits._
    flights.groupBy("passengerId")
      .count()
      .withColumnRenamed("count", "numberOfFlights")
      .orderBy(desc("numberOfFlights"))
      .limit(100)
      .join(passengers, "passengerId")
      .select(
        col("passengerId"),
        col("numberOfFlights"),
        col("firstName"),//cant use withColumnRenamed on top since its from other ds
        col("lastName")
      )
      .as[TopFlyer]
      //.select("passengerId", "Number of Flights", "First name", "Last name")
      //.write.option("header", true).mode("overwrite").csv(outputPath)
  }

  //greatest number of countries a passenger has been in without being in the UK
  //template answer should be 2,when exclude UK->FR
  //consecutive/streak visits without running to UK,need max,curr
  def question3(flights: Dataset[Flight])(implicit spark: SparkSession): Dataset[LongestRunStat] = {
    //val maxNonUK_Flights = flights.filter("from != 'UK' AND to != 'UK'")
    import flights.sparkSession.implicits._

      flights
      .groupByKey(_.passengerId)
      .mapGroups { case (passengerId, records) => //allow custom process a group using iterator
        val countries = records.toList.map(_.to)///list of countries will be visited/visiting by that passenger

        // method used to reduce a collection into a single result
        val longestRun = countries.foldLeft((0, 0))
        {
          case ((max, curr), country) =>
            //math.max return larger of two numbers a and b
            if (country == "UK") (math.max(max, curr), 0) // reset curr 0,update max with curr if larger
            else (max, curr + 1) // add streak
        }
        match
        {
          case (max, curr) => math.max(max, curr) //update streak that longer or when list not end with uk
        }

        //(passengerId, maxRun)
        LongestRunStat(passengerId, longestRun)//return back as the cc
      }
      //.toDF("passengerId", "longestRun")
      .orderBy(desc("longestRun"))
    //result.write.option("header", true).mode("overwrite").csv(outputPath)

  }



  def question4(flights:Dataset[Flight])(implicit spark: SparkSession) :Dataset[SharedFlight] = {
    import flights.sparkSession.implicits._

    flights.as("f1")
      .join(flights.as("f2"),
        $"f1.flightId" === $"f2.flightId" && $"f1.passengerId" < $"f2.passengerId"
      )
      .groupBy($"f1.passengerId".as("passengerId"), $"f2.passengerId".as("passengerId2"))
      .agg(count("*").as("sharedFlights"))
      .filter($"sharedFlights" > 3)
      .orderBy(desc("sharedFlights"))
      .select(
        col("passengerId"),
        col("passengerId2"),
        col("sharedFlights").as("numberOfFlights")//Spark needs column names to exactly match the case class fields
      )
      .orderBy(desc("numberOfFlights"))
      .as[SharedFlight]
  }

  //Find the passengers who have been on more than N flights together within the range (from,to).
  //same idea as q4,with extra param. from,to,numFlights
  def question5(flights: Dataset[Flight],sharedFlight : Int, fromDate : Date, untilDate : Date)(implicit spark: SparkSession): Dataset[SharedFlightRange] = {
    import flights.sparkSession.implicits._

    val filterFlights = flights
      .withColumn("dateParsed", to_date($"date", "yyyy-MM-dd"))
      .filter($"dateParsed".between(fromDate, untilDate))
      // withColumn returns a untyped DataFrame,adding this will converts df back into a typed Dataset[Flight] allowing using other method like groupbykey
      .as[Flight]


    filterFlights.as("f1")
      .join(filterFlights.as("f2"),
        $"f1.flightId" === $"f2.flightId" && $"f1.passengerId" < $"f2.passengerId"
      )
//      .withColumn("dateParsed", to_date($"date", "yyyy-MM-dd"))
//      .filter($"dateParsed".between(fromDate, untilDate))
      .groupBy($"f1.passengerId".as("passengerId"), $"f2.passengerId".as("passengerId2"))
      .agg(count("*").as("numberOfFlights"))
      .filter($"numberOfFlights" > sharedFlight)
      .orderBy(desc("numberOfFlights"))
//      .select(
//        col("passengerId"),
//        col("passengerId2"),
//        col("sharedFlights").as("numberOfFlights"), //Spark needs column names to exactly match the case class fields
//      )
      .orderBy(desc("numberOfFlights"))
      .map(row => SharedFlightRange(
        row.getAs[Int]("passengerId"),
        row.getAs[Int]("passengerId2"),
        row.getAs[Long]("numberOfFlights"),
        fromDate,
        untilDate
      ))
      //.as[SharedFlightRange] cant because fromDate and untilDate are not in columns
  }

}
