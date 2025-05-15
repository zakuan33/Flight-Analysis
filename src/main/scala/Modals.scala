import java.sql.Date

/*
Case Classes to implement Typed Datasets for input and output
data
*/


//inputs
case class Passenger(passengerId: Int, firstName: String, lastName: String)//fields
case class Flight(passengerId: Int, flightId: Int, from: String, to: String, date: String)

//outputs
case class MonthlyFlightStat(Month: Int, numberOfFlights: Long)
case class TopFlyer(passengerId: Int, numberOfFlights: Long, firstName: String, lastName: String)
case class LongestRunStat(passengerId: Int, longestRun: Int)
case class SharedFlight(passengerId: Int,passengerId2: Int , numberOfFlights: Long)
case class SharedFlightRange(passengerId: Int,passengerId2: Int , numberOfFlights: Long, From : Date, To : Date)