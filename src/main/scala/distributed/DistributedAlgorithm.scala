package distributed
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

class DistributedAlgorithm(sc: SparkContext) extends Serializable{

  def count(input: RDD[(Int, ListBuffer[(Int, Double)])], inputFile: String): RDD[(Int, Int, Int, Double)] ={
    // for every node keeps a list with all nodes that the specific node is connected with and their probabilities
    var reducer1 = input.reduceByKey((a, b) => a++b)
      .mapPartitions(partition =>
        partition.map(x => {
          var onevsrest = x._2.combinations(2).to[ListBuffer]
          if(!onevsrest.isEmpty) {
            onevsrest = onevsrest.distinct
          }
          (x._1, onevsrest)
        })
      )
      .filter(x => x._2.length > 1)
      .flatMap(x => {
        x._2.map(y => {
          val node2 = y(0)._1
          val node3 = y(1)._1
          if (node2 < node3) {
            ((node2.toString + ':' + node3.toString), (x._1, node2, node3, y(0)._2 * y(1)._2))
          } else {
            ((node3.toString + ':' + node2.toString), (x._1, node3, node2, y(0)._2 * y(1)._2))
          }
        })
      })

    val newForm = sc.textFile(inputFile)
      .repartition(8)
      .map(x=> x.split(" "))
      // sort edges in ascending order (based on first node)
      .map(p =>{
        if (p(0).toInt < p(1).toInt)
          ((p(0) + ":" + p(1)), (p(0).toInt, p(1).toInt, p(2).toDouble))
        else
          ((p(1) + ":" + p(0)), (p(1).toInt, p(0).toInt, p(2).toDouble))
      })

    var pairPossibleTriangles = reducer1.sortBy(_._1)
    var pairNodes = newForm.sortBy(_._1)

    val joined = pairPossibleTriangles.join(pairNodes).map(p=> {
      (p._1,p._2._1, p._2._2)
    })

    // check the closing probability of the triangle
    val output = joined.mapPartitions(partition => {
      partition.map(x => {
        var prob_triangle = x._2
        var closing_nodes_prob = x._3._3
        var final_prob = prob_triangle._4 * closing_nodes_prob
        (prob_triangle._1, prob_triangle._2, prob_triangle._3, final_prob)
      })
    })
    output
  }
}
