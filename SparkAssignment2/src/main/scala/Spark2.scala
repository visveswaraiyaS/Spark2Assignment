import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, _}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{GBTClassifier, LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.types.StringType


object Spark2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Initializing spark session
  val spark = SparkSession.builder().appName("Spark2").master("local[*]").getOrCreate()

  // Creating dataframe from the .csv titanic train file
  val trainDF: DataFrame = spark.read.option("header", true).option("inferSchema", true).csv("/Users/surajvisvesh/Documents/courses/csye7200-big-data/assignment/assignment-spark2/train.csv")

  // DATA CLEANSING

  // Counting null values in each column
  val nullCounts = trainDF.select(trainDF.columns.map(c => sum(when(col(c).isNull, 1).otherwise(0)).alias(c)): _*)
  nullCounts.show()

  // finding mean to replace the null values at age column
  val averageAge = trainDF.select("Age").agg(avg("Age")).collect() match {
    case Array(Row(avg: Double)) => avg
    case _ => 0
  }
  val roundedMeanAge:Double = BigDecimal(averageAge).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
  println(roundedMeanAge)

  // new df with null values replaced with average age
  val trainDF1 = trainDF.na.fill(roundedMeanAge, Seq("Age"))
  trainDF1.show()

  val trainDF2 = trainDF1.na.fill("s", Seq("Embarked"))


  val testDF = spark.read.option("header", true).option("inferSchema", true).csv("/Users/surajvisvesh/Documents/courses/csye7200-big-data/assignment/assignment-spark2/test.csv")
  val averageAgeTest = testDF.select("Age").agg(avg("Age")).collect() match {
    case Array(Row(avg: Double)) => avg
    case _ => 0
  }
  val roundedMeanAgeTest: Double = BigDecimal(averageAgeTest).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
  println(roundedMeanAgeTest)

  val averageFareTest = testDF.select("Age").agg(avg("Age")).collect() match {
    case Array(Row(avg: Double)) => avg
    case _ => 0
  }

  val roundedMeanFareTest: Double = BigDecimal(averageFareTest).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
  println(roundedMeanFareTest)

  val testDF1 = testDF.na.fill(roundedMeanAge, Seq("Age")).na.fill(roundedMeanFareTest, Seq("Fare"))
  val testDF2 = testDF1.na.fill("s", Seq("Embarked"))
  val testDF3 = testDF2.drop("Cabin", "Ticket")


  // INITIAL CLEANSING OF DATASET IS OVER
  // IMPLEMENTED FEATURE ENGINEERING - Tite and HasCabin COLUMNS HAVE BEEN ADDED

  // EXPLAINING A FEW STATISTICS - EDA

  // Finding mean and standard deviation.
  val meanMedianSD = trainDF2.select(mean("Fare").alias("mean fare"),stddev("Fare").alias("Standard Deviation of Fare"),
    mean("Age").alias("Mean Age"), stddev("Age").alias("Standard Deviation of Age"))
  meanMedianSD.show()
  // When normally distributed, about 68% of the population lie between the age

  // Finding average ticket price of the passengers.
  val averageFare = trainDF2.select("Fare").agg(avg("Fare")).collect() match {
    case Array(Row(avg: Double)) => avg
    case _ => 0
  }
  println("Average fare of the Titanic ship - $" + averageFare)

  // Finding average ticket price from where they embarked
  val averageFareEmbarked = trainDF2.filter(col("Embarked").isNotNull)
    .groupBy("Embarked")
    .agg(avg("Fare"))
    .alias("Average Ticket Fare")
  averageFareEmbarked.show()
  // This tells us that Q might be the last stop before crashing(closest stop to destination) because it has the least average fare price,
  // while C might be the departure location, having the highest average ticket fare


  // Finding the overall survival percentage
  val survivalRate: Double = (trainDF2.filter(col("Survived") === 1).count().toDouble / trainDF2.count()) * 100
  println("Survival Rate of the ship: " + BigDecimal(survivalRate).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble + "%")

  // Finding the survival rate of Sex
  val survivalGender = trainDF2.filter(col("Survived") === 1).groupBy("Sex").count().withColumnRenamed("count","SurvivedPassengers").
    join(trainDF2.groupBy("Sex").count().withColumnRenamed("count","TotalPassengers"), Seq("Sex"))
  val survivalRateGender = survivalGender.withColumn("SurvivalRate", col("SurvivedPassengers") / col("TotalPassengers"))
  survivalRateGender.show()
  // This shows us that women were evacuated first during the crisis

  //Finding the survival rate of children below 15 years
  val survivalChildren = (trainDF2.filter(col("Age") <= 12 && col("Survived") === 1).count().toDouble / trainDF2.filter(col("Age") <= 12).count()) * 100
  println("Survival rate of Children: "+survivalChildren + "%")


  // FEATURE ENGINEERING


  val trainDF3 = trainDF2.withColumn("Title", regexp_extract(col("Name"), "(Mr\\.|Mrs\\.|Miss\\.|Master\\.|Col\\.)", 0))
  val trainDF4 = trainDF3.drop("Cabin", "Ticket")

//  val testDF3ColNames = Seq("PassengerId", "Pclass", "Name", "Sex", "Age", "SibSp", "Parch", "Fare", "Embarked")
  val testDF4 = testDF3.withColumn("Survived", lit("0")).select("PassengerId", "Survived", "Pclass", "Name", "Sex", "Age", "SibSp", "Parch", "Fare", "Embarked")
  val testDF5 = testDF4.withColumn("Title", regexp_extract(col("Name"), "(Mr\\.|Mrs\\.|Miss\\.|Master\\.|Col\\.)", 0))
  testDF5.show()



  val allData = trainDF4.union(testDF5)
  allData.cache()

  val categoryFeatures = Seq("Pclass", "Sex", "Embarked", "Title")
  val stringIndexers = categoryFeatures.map { colName =>
    new StringIndexer()
      .setInputCol(colName)
      .setOutputCol(colName + "Indexed")
      .fit(trainDF4)
  }


  //Indexing target feature
  val labelIndexer = new StringIndexer()
    .setInputCol("Survived")
    .setOutputCol("SurvivedIndexed")
    .fit(trainDF4)

  //Assembling features into one vector
  val numericFeatures = Seq("Age", "SibSp", "Parch", "Fare")
  val categoryIndexedFeatures = categoryFeatures.map(_ + "Indexed")
  val IndexedFeatures = numericFeatures ++ categoryIndexedFeatures
  val assembler = new VectorAssembler()
    .setInputCols(Array(IndexedFeatures: _*))
    .setOutputCol("Features")

//Randomforest classifier
  val randomforest = new RandomForestClassifier()
    .setLabelCol("Survived")
    .setFeaturesCol("Features")

  //Retrieving original labels
  val labelConverter = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictedLabel")
    .setLabels(labelIndexer.labels)

  //Creating pipeline
  val pipeline = new Pipeline().setStages(
    (stringIndexers :+ labelIndexer :+ assembler :+ randomforest :+ labelConverter).toArray)

  trainDF4.show()
  testDF5.show()

  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("SurvivedIndexed")
    .setPredictionCol("prediction")

  val model = pipeline.fit(trainDF4)

  //predictions on validation data
  val predictions = model.transform(testDF5)
  predictions.show(false)

  //Accuracy
  val accuracy = evaluator.evaluate(predictions)
  println("Accuracy of Titanic Train and Test: " + accuracy * 100)

  val survivalGender2 = predictions.filter(col("Prediction") === 1).groupBy("Sex").count().withColumnRenamed("count", "SurvivedPassengers").
    join(predictions.groupBy("Sex").count().withColumnRenamed("count", "TotalPassengers"), Seq("Sex"))
  val survivalRateGender2 = survivalGender2.withColumn("SurvivalRate", col("SurvivedPassengers") / col("TotalPassengers") * 100)
  survivalRateGender2.show()

  spark.stop()
}
