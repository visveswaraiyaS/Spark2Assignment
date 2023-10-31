ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "SparkAssignment"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.4.1"