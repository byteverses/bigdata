package base

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}

object UsrWatchList {

    val usrDataFile = "spark/spark-tourist/src/main/resources/user_item_score.data"

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.ERROR)

        // Spark RDD
        rddTopN


        // Spark-sql:
        val sparkSession = SparkSession.builder.appName("uwl").master("local[*]").getOrCreate

        val usrData = sparkSession.read.option("delimiter", "\t").csv(usrDataFile)
                                  .select(col("_c0").alias("usr"),
                                          col("_c1").alias("item"),
                                          col("_c2").cast(DoubleType).alias("score"))

        val count = usrData.count()
        usrData.sort("usr").repartition(col("usr")).sortWithinPartitions("score")

    }

    def rddTopN = {
        val conf = new SparkConf().setMaster("local").setAppName("uwl")
        val sc = new SparkContext(conf)
        val textFile = sc.textFile(usrDataFile)

        val topN = 5

        textFile.map(_.split("\t"))
                .map(line => (line(0), (line(1), line(2).toInt)))
                .groupByKey()
                .sortByKey()
                .map(one => (one._1, one._2.toStream.sortBy(_._2).reverse.take(topN).toList))
                .foreach(println(_))
    }
}
