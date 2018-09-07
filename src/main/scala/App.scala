import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Period}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object App {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = ConfigFactory.load()
    val sConf = new SparkConf()
      .setMaster(conf.getString("spark.master"))
      .setAppName(conf.getString("spark.app-name"))
    val spark = new SparkContext(sConf)

    val partitionsNb = 17
    val skewedRdd = spark.parallelize(0 to partitionsNb, partitionsNb).flatMap(i =>
      0 until Math.exp(i).toInt
    ).mapPartitionsWithIndex((i, p) => p.map((i, _))).cache()
    val smallRdd = spark.parallelize(0 to partitionsNb, partitionsNb).flatMap(i =>
      0 until i
    ).mapPartitionsWithIndex((i, p) => p.map((i, _))).cache()

    printRdd(skewedRdd)
    printRdd(smallRdd)

    println(s"Total count skewed ${skewedRdd.count()}")
    println(s"Total count small ${smallRdd.count()}")

    //intersectRdds(skewedRdd, smallRdd)

    val n = 100
    val smallRddTransformed = smallRdd
      .cartesian(spark.parallelize(0 until n))
      .map(x => ((x._1._1, x._2), x._1._2)).cache()

    val skewedRddTransformed = skewedRdd
      .map(x => ((x._1, Random.nextInt(n - 1)), x._2)).cache()

    println("<<<<<<< skewedRddTransformed >>>>>>>")
    skewedRddTransformed.take(100).foreach(println)
    println("<<<<<<< smallRddTransformed >>>>>>>")
    smallRddTransformed.take(100).foreach(println)

    val now = DateTime.now()
    val res = skewedRddTransformed.leftOuterJoin(smallRddTransformed)
    res.count()
    println(s"Time elapsed: " + new Period(now, DateTime.now()))

    spark.stop()
  }

  private def printRdd[T](rdd: RDD[T]): Unit = {
    println(s"rdd has ${rdd.getNumPartitions} partitions")

    rdd.mapPartitionsWithIndex((i, p) => {
      println(s"partition $i containing ${p.size} values")
      Iterator(i, p.size)
    }).collect
  }

  private def intersectRdds(skewedRdd: RDD[(Int, Int)], smallRdd: RDD[(Int, Int)]): Unit = {
    val now = DateTime.now()

    val res = skewedRdd.leftOuterJoin(smallRdd)
    res.count()

    println(s"Time elapsed: " + new Period(now, DateTime.now()))
  }
}