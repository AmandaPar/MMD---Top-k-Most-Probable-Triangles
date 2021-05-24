import basic.{BasicAlgorithm, Edge, Triangle}
import distributed.DistributedAlgorithm
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

object Driver {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Count Triangles")
      .setMaster("local[8]")
    val sc = new SparkContext(conf)

    // Read text file into RDD
    val inputFile = "./youtube.txt"
    val data = sc.textFile(inputFile)
      .repartition(8)
      .map(x=> x.split(" "))
      // sort edges in ascending order (based on first node)
      .map(p =>{
        if (p(0).toInt < p(1).toInt)
          Edge(p(0).toInt, p(1).toInt, p(2).toDouble)
        else
          Edge(p(1).toInt, p(0).toInt, p(2).toDouble)
      })

    var input = sc.textFile(inputFile)
      .repartition(8)
      .map(x=> x.split(" "))
      .map(p =>{
        if (p(0).toInt < p(1).toInt)
          (p(0).toInt, ListBuffer((p(1).toInt, p(2).toDouble)))
        else {
          (p(1).toInt, ListBuffer((p(0).toInt, p(2).toDouble)))
        }
      })

    data.persist()
    input.persist()
    print("Welcome! \n")
    println("Choose the algorithm you want to run")

    println("Type:")
    println("1 for Basic Algorithm")
    println("2 for Distributed Algorithm")

    val task = scala.io.StdIn.readLine().toInt
    val k = 100
    val start = System.nanoTime()

    task match {
      case 1 =>
        val basicAlgo = new BasicAlgorithm(sc)
        val triangles: ListBuffer[Triangle] = basicAlgo.count(data)
        triangles.sortWith(_.prob > _.prob).take(k).foreach(println)
      case 2 =>
        val nodeIter = new DistributedAlgorithm(sc)
        // nodeIterator need as input (key,value), where key is vertex-id and value is adjacency list of vertex
        val final_triangles = nodeIter.count(input, inputFile)
        final_triangles.sortBy(_._4, false).take(k).foreach(println)
    }
    println("Total Execution Time: " + (System.nanoTime - start) / 1e9d + "seconds")
  }
}
