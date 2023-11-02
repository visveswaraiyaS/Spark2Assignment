ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "SparkAssignment2"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.0"
