import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.log4j.{Level, Logger}


object Spark2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("Spark2").master("local[*]").getOrCreate()
  val trainDataset: DataFrame = spark.read.option("header", true).csv("/Users/surajvisvesh/Documents/courses/csye7200-big-data/assignment/assignment-spark2/train.csv")
  trainDataset.show(100)
  val numRows = trainDataset.count()
  println(numRows)
  spark.stop()
}

