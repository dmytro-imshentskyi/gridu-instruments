package gridu

import java.sql.Date
import java.time.Month

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Demo {

  def main(args: Array[String]): Unit = {
   implicit val spark: SparkSession = SparkSession.builder()
      .master("local[*]").appName("home-depot-spark-app").getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val schema = defineSchema()

    val instruments = spark.read
      .schema(schema)
      .option("header", value = true)
      .option("dateFormat", "dd-MMM-yyyy")
      .csv(filePath).as[Instrument]

    val upToDateInstrumentsDf = instruments.filter(instruments => isUpToNow(instruments)).cache()

    //TASK #1. For INSTRUMENT1 calculate mean
    printResult(meanForInstrument1(upToDateInstrumentsDf))

    //TASK #2.For INSTRUMENT2 – mean for November 2014
    printResult(meanForInstrument2(upToDateInstrumentsDf))

    //TASK #3. INSTRUMENT3 – any other statistical calculation that we can compute "on-the-fly" as we read the file
    println("Median value for INSTRUMENT3 is " + meanForInstrument2(upToDateInstrumentsDf))

    //TASK #4. For any other instrument from the input file - sum of the newest 10 elements
    printResult(topNInstruments3(upToDateInstrumentsDf, 10))
  }

  def meanForInstrument1(ds: Dataset[Instrument])(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    ds.filter(item => "INSTRUMENT1".contentEquals(item.id))
      .groupBy($"id").agg(round(mean($"value"), 4).alias("mean"))
  }

  def meanForInstrument2(ds: Dataset[Instrument])(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    ds.filter(item => "INSTRUMENT2".contentEquals(item.id) && Month.NOVEMBER.equals(item.date.toLocalDate.getMonth))
      .groupBy($"id")
      .agg(round(mean($"value"), 4).alias("mean"))
  }

  def medianForInstrument3(ds: Dataset[Instrument]): Double = {
    ds.filter(item => "INSTRUMENT3".contentEquals(item.id))
      .stat.approxQuantile("value", Array(0.5), 0.0)(0)
  }

  def topNInstruments3(ds: Dataset[Instrument], n: Int)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    ds.filter(item => "INSTRUMENT3".contentEquals(item.id))
      .withColumn("rank", rank().over(Window.orderBy($"date".desc))).limit(n)
  }

  private[this] def filePath = {
    val resource = Demo.getClass.getClassLoader.getResource("example_input.csv")
    resource.getPath
  }

  private[this] def printResult(df: DataFrame): Unit = df.show(truncate = false)

  private[this] def isUpToNow(instrument: Instrument): Boolean = instrument.date.compareTo(Date.valueOf("2014-12-19")) == -1

  private[this] def defineSchema(): StructType =
    StructType.apply(Seq(StructField("id", StringType),
      StructField("date", DateType),
      StructField("value", DoubleType)))
}
