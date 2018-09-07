import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, Period}
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.util.Random

object App {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = ConfigFactory.load()
    val spark = SparkSession.builder.appName(conf.getString("spark.app-name")).master(conf.getString("spark.master")).getOrCreate()

    val partitionsNb = 17
    val skewedRdd = spark.sparkContext.parallelize(0 to partitionsNb, partitionsNb).flatMap(i =>
      0 until Math.exp(i).toInt
    ).cache()
    val smallRdd = spark.sparkContext.parallelize(0 to partitionsNb, partitionsNb).flatMap(i =>
      0 until i
    ).cache()

    printRdd(skewedRdd)
    printRdd(smallRdd)

    println(s"Total count skewed ${skewedRdd.count()}")
    println(s"Total count small ${smallRdd.count()}")

//    intersectRdds(skewedRdd, smallRdd)

    val n = 100
    val smallRddTransformed = smallRdd
      .mapPartitionsWithIndex((i, p) => p.map((i, _)))
      .cartesian(spark.sparkContext.parallelize(0 until n))
      .map(x => ((x._1._1, x._2), x._1._2)).cache()

    val skewedRddTransformed = skewedRdd
      .mapPartitionsWithIndex((i, p) => p.map((i, _)))
      .map(x => ((x._1, Random.nextInt(n - 1)), x._2)).cache()

    println("<<<<<<< skewedRddTransformed >>>>>>>")
    skewedRddTransformed.take(100).foreach(println)
    println("<<<<<<< smallRddTransformed >>>>>>>")
    smallRddTransformed.take(100).foreach(println)

    intersectRdds(skewedRddTransformed, smallRddTransformed)

    spark.stop()
  }

  private def printRdd(rdd: RDD[Int]): Unit = {
    println(s"rdd has ${rdd.getNumPartitions} partitions")

    rdd.mapPartitionsWithIndex((i, p) => {
      println(s"partition $i containing ${p.size} values")
      Iterator(i, p.size)
    }).collect
  }

  private def intersectRdds[T](skewedRdd: RDD[T], smallRdd: RDD[T]): Unit = {
    val now = DateTime.now()

    val res = skewedRdd.(smallRdd)
    res.count()

    println(s"Time elapsed: " + new Period(now, DateTime.now()))
  }
}