package tc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RepJoinRDDMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nfc.RepJoinRDDMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Rep-R Triangle Count")
    val sc = new SparkContext(conf)
    val MAX_VALUE = 12000
    val accumulator = sc.longAccumulator("Triangle Cumulative Count")

    // Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================
    
    val textFile = sc.textFile(args(0))

    // create edge RDD
    val edges = textFile
                 .map(edge => edge.split(",")) // split edge to get nodes
                 .map(nodes => (nodes(0).toInt, nodes(1).toInt)) // map nodes to (followerID, followedID)
                 .filter{case (from, to) => from < MAX_VALUE && to < MAX_VALUE} // only get node IDs less than custom set max value

    // create edge map which has set of followedIDs for each followerID
    val edgeMap = edges
                  .map{case (from, to) => (from, Set(to))}
                  .reduceByKey(_ ++ _) // reduce by followerID to get set of followedIDs
                  .collectAsMap()

    // create broadcast variable map
    val broadcastMap = sc.broadcast( edgeMap )

    // calculate triangle count
    val triangleCount = edges.mapPartitions(iter => {
                          iter.flatMap {
                            case (nodeX, nodeY) => broadcastMap.value.get(nodeY).map( // for each Y followed by X, get set of IDs Y is following
                              setY => setY.foreach(nodeZ => broadcastMap.value.get(nodeZ).foreach( // for each Z followed by Y, get set of IDs Z is following
                                setZ => if (setZ.contains(nodeX)) { // if Z follows X, triangle has been formed; increment the accumulator
                                  accumulator.add(1)
                                }
                              )
                            )
                          )
                        }
                      }
                    ).collect()

    logger.info("-------------------------------------------")
    logger.info(broadcastMap.value.get(1))
    logger.info(accumulator)
    logger.info("triangle count: " + accumulator.value / 3)
    logger.info("-------------------------------------------")
  }
}
