package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object FollowerCountMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.FollowerCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Follower Count")
    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    
    val textFile = sc.textFile(args(0))  // args(0) = input path. read the input csv file.
    val counts = textFile.flatMap(edges => edges.split("\n"))  // split the input by whitespace "\n" and flatten the split edges to create a single RDD.
                 .map(edge => edge.split(",")(1))  // for each nodes, split by "," and get the second element which is the Twitter ID being followed.
                 .map(userID => (userID, 1))  // map each of the userID to create the pair (userID, 1).
                 .reduceByKey(_ + _)  // sum up the count for each correspondig userID.
                 .filter{case (userID, count) => count % 100 == 0}  // only get the userID with number of followers that can be divisble by 100.
    logger.info("counts log starts here")
    logger.info(counts.toDebugString)
    logger.info("counts log ends here")
    counts.saveAsTextFile(args(1))  // args(1) = output path. write the results (userID, total followers count) as the text file.
  }
}