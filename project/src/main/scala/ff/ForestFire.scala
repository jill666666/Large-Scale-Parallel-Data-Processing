package ff

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object ForestFireMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nff.ForestFireMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Forest Fire")
    val sc = new SparkContext(conf)
    
    val textFile = sc.textFile(args(0))

    val rnd = new scala.util.Random

    /** user-defined parameters **/
    val p = 0.97  // forward burning probability
    val numSeeds = 150  // initial number of seed nodes

    val mean = p / (1 - p)  // Gaussian distribution mean

    val numPartitions = 50.min(numSeeds * 10)
    val partitioner = new HashPartitioner(numPartitions)

    // i. get outlinks of each node
    val outlinks = textFile
                   .map(line => line.split(" "))
                   .map(nodes => (nodes(0).toInt, nodes(1).toInt))
                   .map{case (from, to) => (from, Set(to))} // (node, set of outlinks)
                   .reduceByKey(_ ++ _)
                   .partitionBy(partitioner)
                   .cache()

    val totalNumNodes = outlinks.count()

    // ii. choose multiple seed nodes uniformly at random
    val seedNodes = outlinks
                     .takeSample(withReplacement=false, numSeeds, seed=42)
                     .map{case(seed, _) => (seed, false)} // (seed node, flag 'explored')

    var exploration = sc.parallelize(seedNodes)

    var samples = sc.emptyRDD[(Int, Int)] // (from, to)
    var newSamples = sc.emptyRDD[(Int, Int)] // (from, to)

    // iterate the process until "enough" nodes are sampled
    for (iter <- 1 to 12) {

      // iii. burn neighbor nodes and collect samples
      newSamples = exploration
                   .filter{case(_, explored) => explored == false} // filter out explored nodes
                   .leftOuterJoin(outlinks)
                   .filter{case(_, (_, links)) => links != None} // filter out dangling nodes
                   .map{case(node, (flag, Some(set))) => 
                      // sample x outlinks incident to exploring node,
                      // where number x is Gaussian distributed with mean p / (1 - p)
                      rnd.shuffle(set).take((rnd.nextGaussian() + mean).toInt).map(w => (node, w))}
                   .flatMap(identity) // flatten sampled outlinks set

      samples = samples.union(newSamples) // update samples

      // iv. update exploration
      exploration = samples
                    .map{case(from, to) => (to, false)}
                    .reduceByKey{case(node, _) => node}
                    .fullOuterJoin(exploration)
                    .map{case(node, (prevFlag, currFlag)) => {
                      // update flag 'explored'
                      if (currFlag == None) {
                        (node, false)
                      } else {
                        (node, true)
                      }
                }
          }
    }

    val sampleCount = samples.count()

    logger.info("sampling done")
    
    // save output in specific format (e.g., "1,2")
    samples.coalesce(6).map{case(from, to) => s"${from},${to}"}.saveAsTextFile(args(1))

    // statistics
    println("------------------------")
    println("# input nodes with outlink(s): " + totalNumNodes)
    println("# seed nodes: " + numSeeds)
    println("# partitions: " + numPartitions)
    print("seed nodes: ")
    seedNodes.foreach{case(seed, _) => print(seed + " ")}
    println("\n# sampled edges: " + sampleCount)
    println("------------------------")

  }
}
