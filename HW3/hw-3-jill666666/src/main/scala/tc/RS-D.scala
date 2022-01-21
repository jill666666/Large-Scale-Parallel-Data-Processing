package tc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RSJoinDataFrameMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nfc.RSJoinDataFrameMain <input dir> <output dir>")
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

    val spark = SparkSession.builder().appName("RS-D").getOrCreate()

    import spark.implicits._

    // create edge RDD
    val edgeRDD = textFile
                .map(edge => edge.split(",")) // split edge to get nodes
                .map(nodes => (nodes(0).toInt, nodes(1).toInt)) // map nodes to (followerID, followedID)
                .filter{case (from, to) => from < MAX_VALUE && to < MAX_VALUE} // only get node IDs less than custom set max value

    // convert edge RDD to (X->Y) DataFrame
    val XtoYDF = edgeRDD.toDF("X", "Y")
    
    // construct (Y -> Z) DataFrame
    val YtoZDF = XtoYDF.toDF("Y", "X").withColumnRenamed("X", "Z")

    // construct (Z -> X) DataFrame
    // this represents candidates for valid closing edges to form triangle
    val ZtoXDF = XtoYDF.join(YtoZDF, XtoYDF("Y") === YtoZDF("Y") && XtoYDF("X") =!= YtoZDF("Z")).select("Z", "X")

    // valid closing edges which form triangle with given nodes
    val closingEdgeDF = XtoYDF
                    .withColumnRenamed("Y", "Z")
                    .as("XtoZ")
                    .join(ZtoXDF.as("ZtoX"), $"XtoZ.X" === $"ZtoX.Z" && $"XtoZ.Z" === $"ZtoX.X") // ensure followerID & followedID equality

    val closingEdgeCount = closingEdgeDF.count()

    val triangleCount = closingEdgeCount / 3 // remove duplicate counts

    logger.info("-------------------------------------------")
    logger.info("closing edge count: " + closingEdgeCount)
    logger.info("triangle count: " + triangleCount)
    logger.info("-------------------------------------------")

    // val YtoZRDD = XtoYRDD
    //                 .map{case (from, to) => (to, from)}

    // val ZtoXRDD = XtoYRDD
    //             .join(YtoZRDD)
    //             .map{case (_, (from, to)) => (from, to)}

    // val triangles = XtoYRDD
    //                 .join(ZtoXRDD)
    //                 .filter{case (_, (to1, to2)) => to1 == to2}
    //                 .map{case (from, (to1, _)) => (from, to1)}

    // val triangleCount = triangles.count() / 3

    // println("-------------------------------------------")
    // println("number of triangles: " + triangleCount)
    // println("-------------------------------------------")
  }
}
