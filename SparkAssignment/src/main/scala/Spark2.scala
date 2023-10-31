import org.apache.spark.sql.SparkSession

object Spark2 extends App {
    val spark = SparkSession.builder().appName("Spark2").getOrCreate()
    val trainDataset = spark.read.option("header", true).csv("/Users/surajvisvesh/Documents/courses/csye7200-big-data/assignment/assignment-spark2/train.csv")
    trainDataset.show(100)
    val numRows = trainDataset.count()
    println(numRows)
    spark.stop()

}