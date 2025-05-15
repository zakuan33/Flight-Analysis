ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "Quantexa_Assn_Zakuan_Nafis"
  )

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8"

)