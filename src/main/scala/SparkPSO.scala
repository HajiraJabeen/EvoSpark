

import scala.math.random
import Array._
import org.apache.spark._
import org.apache.spark.rdd.RDD
object SparkPSO {
/** Computes an approximation to pi */


  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Spark Pso").setMaster("local[2]")
    val spark = new SparkContext(conf)
    
    val slices = 10// if (args.length > 0) args(0).toInt else 2
    val dim = 30
    var position = (1 to dim toArray) map (x => Math.random())
    var velocity = (1 to dim toArray) map (x => Math.random())
    var pbest    = (1 to dim toArray) map (x => Math.random())
     //sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x)
    val chk = spark.parallelize(position).foreach { x => x+2}
     
    // println(chk(1).toString())
     
     
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    
    val cc = spark.parallelize(init())
    cc.map { x => ??? }
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
def init (): Array[Double] = {
      var myMatrix = ofDim[Double](3)
      myMatrix(1) =random 
      myMatrix(2) =random 
      myMatrix(3) =random 
      
      myMatrix }
}