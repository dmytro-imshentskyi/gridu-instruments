package gridu

import java.sql.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.scalatest.FlatSpec

class InstrumentCsvSpec extends FlatSpec
  with SparkSessionTestWrapper {

  import spark.implicits._

  val instrumentsAsRdd: RDD[Instrument] = spark.sparkContext.parallelize(Seq(
    Instrument("INSTRUMENT1", Date.valueOf("1996-01-01"), 29.19),
    Instrument("INSTRUMENT1", Date.valueOf("1996-01-01"), 31.00),
    Instrument("INSTRUMENT1", Date.valueOf("1996-01-01"), 9.11),
    Instrument("INSTRUMENT1", Date.valueOf("1996-01-01"), 33.39),

    Instrument("INSTRUMENT2", Date.valueOf("1996-01-01"), 29.19),
    Instrument("INSTRUMENT2", Date.valueOf("1996-11-11"), 31.00),
    Instrument("INSTRUMENT2", Date.valueOf("1996-01-01"), 9.11),
    Instrument("INSTRUMENT2", Date.valueOf("1996-11-10"), 13.39),

    Instrument("INSTRUMENT3", Date.valueOf("1996-01-01"), 29.19),
    Instrument("INSTRUMENT3", Date.valueOf("1996-11-01"), 9.11),
    Instrument("INSTRUMENT3", Date.valueOf("1996-01-01"), 29.19),
    Instrument("INSTRUMENT3", Date.valueOf("1996-11-11"), 29.19)))

  val instruments: Dataset[Instrument] = spark.createDataset(instrumentsAsRdd).cache()

  val emptyInstruments: Dataset[Instrument] = spark.emptyDataset[Instrument]

  "Task #1. Calculated mean for INSTRUMENT1" should "be equal to 25.6725" in {
    val result = Demo.meanForInstrument1(instruments)(spark).collectAsList()
    assertResult("INSTRUMENT1")(result.get(0).getAs[String]("id"))
    assertResult(1)(result.size())
    assertResult(25.6725)(result.get(0).getAs[Double]("mean"))
  }

  "Task #2. Calculated mean for INSTRUMENT2 " should "be equal to 22.195" in {
    val result = Demo.meanForInstrument2(instruments)(spark).collectAsList()
    assertResult(1)(result.size())
    assertResult("INSTRUMENT2")(result.get(0).getAs[String]("id"))
    assertResult(22.195)(result.get(0).getAs[Double]("mean"))
  }

  "Task #3. Calculated median for INSTRUMENT3 " should "be equal to 29.19" in {
    val result = Demo.medianForInstrument3(instruments)
    assertResult(Some(29.19))(result)
  }

  "Task #4. First and second INSTRUMENT3 " should "contains values 29.19 and 9.11 respectively" in {
    val result = Demo.topNInstruments3(instruments, 2)(spark).collectAsList()
    assertResult(2)(result.size())
    assertResult("INSTRUMENT3")(result.get(0).getAs[String]("id"))
    assertResult(29.19)(result.get(0).getAs[Double]("value"))
    assertResult(1)(result.get(0).getAs[Int]("rank"))

    assertResult("INSTRUMENT3")(result.get(1).getAs[String]("id"))
    assertResult(9.11)(result.get(1).getAs[Double]("value"))
    assertResult(2)(result.get(1).getAs[Int]("rank"))
  }

  "TASK #1, 2, 3, 4. Calculated values for all tasks with empty dataset" should "be empty" in {
    val result1 = Demo.meanForInstrument1(emptyInstruments)(spark).collectAsList()
    assert(result1.isEmpty)

    val result2 = Demo.meanForInstrument2(emptyInstruments)(spark).collectAsList()
    assert(result2.isEmpty)

    val result3 = Demo.medianForInstrument3(emptyInstruments)
    assertResult(None)(result3)

    val result4 = Demo.topNInstruments3(emptyInstruments, 2)(spark).collectAsList()
    assert(result4.isEmpty)
  }
}
