import models.{Department, User}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{broadcast, monotonically_increasing_id}
import org.joda.time.format.PeriodFormat
import org.joda.time.{DateTime, Period}

import scala.util.{Random, Try}

object App extends SparkSessionWrapper {

  import spark.implicits._

  lazy val departments: Dataset[Department] = spark.createDataset(Department.departments.toList).map(_._2)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.toLevel(conf.getString("spark.log")))

    print("Do you want to load data from json with a path (1) or from memory (2)? [1] ")
    val loadFrom = Try(scala.io.StdIn.readInt()).getOrElse(1)
    if (loadFrom == 1) {
      print("Json file path please? ")
      val path = Try(scala.io.StdIn.readLine()).toOption

      process(path)

    } else if (loadFrom == 2) process(None)

    else stop()
  }

  /**
    * Dataset[User] from json path
    *
    * @return
    */
  private def jsonToDataset(path: String): Dataset[User] = {
    println("Loading data...")
    spark.read.json(path).map(r => User(r.getAs[Long](0).toInt, r.getAs[String](1)))
  }

  /**
    * The hard way of trying to join data
    */
  private def process(path: Option[String]): Unit = {
    val skDataset = path.map(p => jsonToDataset(p).cache()).getOrElse {
      print("How many partitions (11 to 12 at most advised if you don't want to sleep here) [11]? ")
      val partitionsNb = Try(scala.io.StdIn.readInt()).getOrElse(11)
      dataset(partitionsNb, skewed = true).cache()
    }

//    println("Grouping by departmentId...")
//    skDataset.groupBy($"departmentId").count.show

    val dpts = departments.cache()

    askShowDatasetsSize(skDataset)

    askDirectJoinDatasets(skDataset, dpts)

    askOptiJoinDatasets(skDataset, dpts)

    stop()
  }

  /**
    * Gets a user dataset from partition generation rdd
    * is skewed data will exponentially along with partition index
    * partition 16 => e(16) ~= 8 millions users
    *
    * @param partitionsNb the amount of partitions
    * @param skewed       whether skewed or linear
    * @return
    */
  private def dataset(partitionsNb: Int, skewed: Boolean): Dataset[User] = {
    spark.createDataset(spark.sparkContext.parallelize(0 to partitionsNb, partitionsNb).flatMap(i =>
      if (skewed) 0 until Math.exp(i).toInt else 0 until i
    ).mapPartitionsWithIndex((i, p) => p.map(x => User(Department.departments(i).id, (Random.alphanumeric take (Random.nextInt(x + 1) + 10)).mkString("")))))
  }

  /**
    * Just counts lines, beware that it is an ACTION so can take time
    *
    * @param skDataset the data
    */
  private def askShowDatasetsSize(skDataset: Dataset[User]): Unit = {
    print("Do you want to print the datasets sizes (can be time consuming) [y/N]? ")
    val yesNoPrintSize = Try(scala.io.StdIn.readChar()).getOrElse('N')
    if (yesNoPrintSize == 'y') {
      println(s"Total count skewed ${skDataset.count()}")
      println(s"Total count departments ${departments.count()}")
    }
  }

  /**
    * Joins without any optimization the datasets
    *
    * @param skDataset the big one with skewed data user
    * @param dpts      the referential departments
    */
  private def askDirectJoinDatasets(skDataset: Dataset[User], dpts: Dataset[Department]): Unit = {
    print("Do you want to perform a raw join on the 2 datasets? (can be time consuming) [y/N]? ")
    val yesNoJoin = Try(scala.io.StdIn.readChar()).getOrElse('N')
    if (yesNoJoin == 'y') {
      joinSets(skDataset, dpts)
      stop()
    }
  }

  /**
    * Does a simple inner join directly and asses the duration
    *
    * @param skewed the skewed data users
    * @param small  the ref department data
    */
  private def joinSets(skewed: Dataset[User], small: Dataset[Department]): Unit = {
    val now = DateTime.now()

    val res = skewed.joinWith(small, skewed("departmentId") === small("id"))
    res.count()

    println(s"Time elapsed: " + PeriodFormat.getDefault.print(new Period(now, DateTime.now())))
  }

  /**
    * The optimized join with intent to repartition
    * skewed data thanks to a unique id added
    * and then a broadcast of the small dataset to the big one
    *
    * @param skDataset big
    * @param dpts      ref small
    */
  private def askOptiJoinDatasets(skDataset: Dataset[User], dpts: Dataset[Department]): Unit = {
    print("Do you want to perform an optimized join on the 2 datasets? [Y/n]? ")
    val yesNoBetterJoin = Try(scala.io.StdIn.readChar()).getOrElse('Y')
    if (yesNoBetterJoin == 'Y') {
      val skDatasetWithId = skDataset
        .withColumn("rowId", monotonically_increasing_id())
        .repartition($"rowId")
        //        .repartition(10000)
        .map(r => User(r.getAs[Int](0), r.getAs[String](1), Some(r.getAs[Long](3))))

      broadcastJoinSets(skDatasetWithId, dpts)
      stop()
    }
  }

  /**
    * Joins and tells duration for big and small datasets with a broadcast
    *
    * @param skewed the big
    * @param small  guess...
    */
  private def broadcastJoinSets(skewed: Dataset[User], small: Dataset[Department]): Unit = {
    val now = DateTime.now()

    val res = skewed.joinWith(broadcast(small), skewed("departmentId") === small("id"))
    res.count()

    println(s"Time elapsed: " + PeriodFormat.getDefault.print(new Period(now, DateTime.now())))
  }

  /**
    * Stops spark session and main script
    */
  private def stop(): Unit = {
    spark.stop()
    System.exit(1)
  }
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