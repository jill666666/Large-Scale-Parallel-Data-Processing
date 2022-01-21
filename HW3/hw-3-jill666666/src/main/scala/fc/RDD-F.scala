package fc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RDDFoldMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nfc.RDDFoldMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RDD-F Follower Count")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================
    
    val textFile = sc.textFile(args(0))
    val counts = textFile
                 .map(edge => edge.split(",")(1)) // split edge to get followedID
                 .map(followedID => (followedID, 1)) // map each node to (node, 1)
                 .foldByKey(0)(_ + _) // fold by key (followedID) which aggregates the count
                 .filter{case (userID, count) => count % 100 == 0} // get counts divisible by 100

    counts.saveAsTextFile(args(1))

    logger.info(counts.toDebugString)
  }
}
