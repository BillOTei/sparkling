import com.typesafe.config.ConfigFactory
import models.{Department, User}
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Period}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.joda.time.format.PeriodFormat

import scala.util.{Random, Try}

object App extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    Logger.getLogger("org").setLevel(Level.toLevel(conf.getString("spark.log")))

    print("How many partitions (11 to 16 at most advised if you don't want to sleep here) [16]? ")
    val partitionsNb = Try(scala.io.StdIn.readInt()).getOrElse(16)

    val skDataset = dataset(partitionsNb, skewed = true).cache()
    val smDataset = dataset(partitionsNb, skewed = false).cache()
    val departments = spark.createDataset(Department.departments.toList).cache()

    print("Do you want to print the datasets sizes (can be time consuming) [y/N]? ")
    val yesNoPrintSize = Try(scala.io.StdIn.readChar()).getOrElse('N')
    if (yesNoPrintSize == 'y') {
      println(s"Total count skewed ${skDataset.count()}")
      println(s"Total count small ${smDataset.count()}")
      println(s"Total count departments ${departments.count()}")
    }


    print("Do you want to perform a raw join on the 2 datasets? (can be time consuming) [y/N]? ")
    val yesNoJoin = Try(scala.io.StdIn.readChar()).getOrElse('N')
    if (yesNoJoin == 'y') {
      joinSets(skDataset, departments)
      spark.stop()
      System.exit(1)
    }
//***** Attempt to generate own new key and spread data ****//
//    val n = 100
//    val smallRddTransformed = smallRdd
//      .cartesian(spark.sparkContext.parallelize(0 until n))
//      .map(x => ((x._1._1, x._2), x._1._2))
//      .coalesce(partitionsNb).cache()
//
//    val skewedRddTransformed = sRdd
//      .map(x => ((x._1, Random.nextInt(n - 1)), x._2)).cache()
//    println("<<<<<<< skewedRddTransformed >>>>>>>")
//    skewedRddTransformed.take(100).foreach(println)
//    println("<<<<<<< smallRddTransformed >>>>>>>")
//    smallRddTransformed.collect.foreach(println)

//    val now = DateTime.now()
//    val res = skewedRddTransformed.leftOuterJoin(smallRddTransformed)
//    res.count()
//    println(s"Time elapsed: " + PeriodFormat.getDefault.print(new Period(now, DateTime.now())))

    print("Do you want to perform an optimized join on the 2 datasets? [Y/n]? ")
    val yesNoBetterJoin = Try(scala.io.StdIn.readChar()).getOrElse('Y')
    if (yesNoBetterJoin == 'Y') {
      val skDatasetWithId = skDataset
        .withColumn("rowId", monotonically_increasing_id())
        .repartition($"rowId")
        .map(r => User(r.getAs[Int](0), r.getAs[String](1), Some(r.getAs[Long](3))))

      joinSets(skDatasetWithId, departments)
      spark.stop()
      System.exit(1)
    }
  }

  private def joinSets(skewed: Dataset[User], small: Dataset[(Int, Department)]): Unit = {
    val now = DateTime.now()

    val res = skewed.joinWith(small, skewed("departmentId") === small("_1"))
    res.count()

    println(s"Time elapsed: " + PeriodFormat.getDefault.print(new Period(now, DateTime.now())))
  }

  private def dataset(partitionsNb: Int, skewed: Boolean): Dataset[User] = {
    import spark.implicits._
    spark.createDataset(spark.sparkContext.parallelize(0 to partitionsNb, partitionsNb).flatMap(i =>
      if (skewed) 0 until Math.exp(i).toInt else 0 until i
    ).mapPartitionsWithIndex((i, p) => p.map(x => User(Department.departments(i).id, (Random.alphanumeric take (Random.nextInt(x + 1) + 10)).mkString("")))))
  }
}