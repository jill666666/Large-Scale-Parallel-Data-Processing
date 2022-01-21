package fc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import org.apache.spark.sql.functions._

// case class SimpleTuple(id: String, desc: Int)

object DSETMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nfc.DSETMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RDD-A Follower Count")
    val sc = new SparkContext(conf)

    // val spark = SparkSession.builder().getOrCreate();

    // Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    
    val textFile = sc.textFile(args(0))
    val rdd = textFile
                 .map(edge => edge.split(",")(1)) // split edge to get followedID
                 .map(followedID => (followedID, 1)) // map each node to (node, 1)

    logger.info(rdd.toDebugString)

    val spark =  SparkSession.builder().appName("DSET").getOrCreate()

    import spark.implicits._
    val ds =  spark.createDataset(rdd) // convert RDD to DataSet

    val counts = ds
                 .groupBy(ds.col("_1")) // group by the first column (followedID)
                 .agg(sum(ds.col("_2"))) // aggregate the values of second column (count)
                 .filter("sum(_2) % 100 == 0") // get counts divisible by 100

    // logger.info(counts.explain)

    counts.rdd.saveAsTextFile(args(1))

    // counts.show()
    counts.explain()
  }
}
