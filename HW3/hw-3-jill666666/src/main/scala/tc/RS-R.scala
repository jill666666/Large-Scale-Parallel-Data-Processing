package tc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RSJoinRDDMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nfc.RSJoinRDDMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RS Triangle Count")
    val sc = new SparkContext(conf)
    val MAX_VALUE = 12000
    // val accumulator = sc.longAccumulator("Triangle Cumulative Count")

    // Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================
    
    val textFile = sc.textFile(args(0))

    // create (X -> Y) RDD
    val XtoYRDD = textFile
                    .map(edge => edge.split(",")) // split edge to get nodes
                    .map(nodes => (nodes(0).toInt, nodes(1).toInt)) // map nodes to (followerID, followedID)
                    .filter{case (from, to) => from < MAX_VALUE && to < MAX_VALUE} // only get node IDs less than custom set max value

    // create (Y -> Z) RDD
    val YtoZRDD = XtoYRDD
                    .map{case (from, to) => (to, from)} // swap each node's position

    // create (Z -> X) RDD
    // this represents candidates for valid closing edges to form triangle
    val ZtoXRDD = XtoYRDD
                .join(YtoZRDD) // by joining we acquire (X, Z) for each key Y
                .map{case (_, (from, to)) => (from, to)} // (X, Z) -> (Z, X)

    // valid closing edges which form triangle with given nodes
    // check equality between the edges and possible closing edges
    val closingEdges = XtoYRDD // contains all edges (can also use YtoZRDD or any edges RDD)
                    .join(ZtoXRDD) // ensure followerID equality
                    .filter{case (_, (to1, to2)) => to1 == to2} // ensure followedID equality
                    .map{case (from, (to1, _)) => (from, to1)} // get closing edge

    val triangleCount = closingEdges.count() / 3 // remove duplicate counts

    logger.info("-------------------------------------------")
    logger.info("number of triangles: " + triangleCount)
    logger.info("-------------------------------------------")
  }
}
