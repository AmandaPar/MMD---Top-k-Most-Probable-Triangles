package basic
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

class BasicAlgorithm(sc: SparkContext) extends Serializable {

  def count(data: RDD[Edge]): ListBuffer[Triangle] ={

    var nodesBroadcast = sc.broadcast(data.collect)
    var triangles = data.collect().flatMap(n1 => {
      var pot_triangles = new ListBuffer[Triangle]()
      nodesBroadcast.value
        // 1 2 0.4
        // 1 3 0.8
        // 2 3 0.6
        // pairnw to 1 2 kai meta psaxnw san 1o kombo to 2
        .filter(n => n.src == n1.dst)
        .foreach(n => {
          nodesBroadcast.value
            .filter(nn => nn.src == n1.src && nn.dst == n.dst)
            .foreach(nn => {
              pot_triangles.append(Triangle(n1.src, n1.dst, n.dst, n1.prob * n.prob * nn.prob))
            })
        }
        )
      pot_triangles
    })
    val result: ListBuffer[Triangle] = triangles.to[ListBuffer]
    result
  }
}
