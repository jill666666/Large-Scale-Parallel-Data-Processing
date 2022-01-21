package pr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer
import scala.math.pow
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object PageRankMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\npr.PageRankMain <input dir> <output dir> <k> <iteration>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

    val k = 100

    val outlinkListBuffer = ListBuffer[(Int, Int)]()
    val rankListBuffer = ListBuffer[(Int, Double)]((0, 0))

    // create outlink (key: page1, value: page2) ListBuffer
    for (i <- 1 to pow(k, 2).toInt) {
      if (i % k == 0) {
        outlinkListBuffer += ((i, 0))
      } else {
        outlinkListBuffer += ((i, i + 1))
      }
    }

    // generate initial PageRank values
    for (i <- 1 to pow(k, 2).toInt) {
      rankListBuffer += ((i, 1.toDouble / pow(k, 2)))
    }

    val spark = SparkSession.builder().appName("PageRank").getOrCreate()

    import spark.implicits._

    // create outlink RDD
    val outlinkRDD = sc.parallelize(outlinkListBuffer)

    // create inlink RDD and join with outlink RDD
    val inlinkRDD = outlinkRDD
                    .map{case (from, to) => (to, from)}
                    // outer-join with outlink graph to get inlink/outlink of each page
                    .fullOuterJoin(outlinkRDD)
                    // map in appropriate format to help join with PageRank graph later on
                    .map {
                      case(page, (inlink, outlink)) => {
                        if (page == 0) {
                          (inlink.getOrElse(0), page) // handle dummy page
                        } else {
                          (inlink.getOrElse(-1), page) // handle other every page
                        }
                      }
                    }

    // create PageRank RDD
    var rankRDD = sc.parallelize(rankListBuffer)

    val alpha = 0.85

    // iteration until convergence
    val iteration = 3
    for (i <- 1 to iteration) {

      println("------------ Iteration " + i + " -------------------")

      // join edge RDD with PageRank RDD and output according to the page type
      val joinRDD = inlinkRDD
                    .leftOuterJoin(rankRDD)
                    .map {
                      case(outlink, (page, someRank)) => {

                        val pageRank = (1 - alpha) / pow(k, 2)

                        // if the page has no inlink, the value is set to 0 by default
                        val rank = someRank.getOrElse(0.toDouble)

                        if (rank == 0.toDouble) {
                          (page, pageRank)  // handle page with no inlink (only outgoing link)
                        } else if (page == 0) {
                          (page, rank)  // handle dangling page
                        } else {
                          (page, pageRank + alpha * rank) // handle page with outlink
                        }
                      }
                    }
                    .reduceByKey(_ + _) // aggregate to get dangling page PR mass

      // read out total dangling PageRank mass in dummy page 0
      val danglingPageRankSum = joinRDD.lookup(0).head

      // distribute dangling PageRank mass evenly over all real pages
      rankRDD = joinRDD
                .map {
                  case(page, rank) => {
                    if (page == 0) {
                      (page, 0.toDouble)  // if page is dummy, set its PageRank to 0
                    } else {
                      (page, rank + alpha * danglingPageRankSum / pow(k, 2))  // update PageRank
                    }
                  }
                }

      logger.info(rankRDD.toDebugString)

    }



    // check if PageRank values sum up to 1
    val pageRankSum = rankRDD.map(_._2).sum()

    // debug purpose - check sum of PageRank of all the pages
    // println("-------- PageRank TOTAL SUM " + pageRankSum)

    // group to one output file and print first 20 records
    rankRDD.repartition(1).sortByKey().take(40).foreach(println)
  }
}