package com.aksw.spark

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.{Vector => SVector}
import org.apache.spark.mllib.linalg.{Vectors => SVectors}
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.Ordering
import scala.util.Random

object PsoSpark {
  def main(args: Array[String]): Unit = {
    val dim = 4
    val iter = 3
    val populationsize = 5

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)

    var population = sc.parallelize((1 to populationsize) map (x => PSOParticle.random(dim)))
    var gbest = PSOParticle.random(dim)
    var gbestBC = sc.broadcast(gbest)

    for (i <- 1 to iter) {
      population = population.map(x => x.update(gbestBC.value))
      gbest = population.min()(Ordering.by(x => x.fit))
      gbestBC = sc.broadcast(gbest)
    }

    println("Global best:" + gbest)
  }
}

case class PSOParticle(dimension: Int,
                       fit: Double,
                       bestFit: Double,
                       position: SVector,
                       velocity: SVector,
                       bestPosition: SVector) {

  override def toString = s"PSOParticle(dimension=$dimension, fit=$fit, bestFit=$bestFit, position=$position, velocity=$velocity, bestPosition=$bestPosition)"

  def update(gBest: PSOParticle): PSOParticle = {
    val newFit = position.toArray.map(x => x * x).sum
    var newBestFit: Double = bestFit
    var newBestPosition = bestPosition

    if (bestFit > newFit) {
      newBestFit = newFit
      newBestPosition = position.copy
    }

    val c1 = 1.49618
    val c2 = 1.49618
    val W = 0.7298

    val vel = new DenseVector(velocity.toArray)
    val pos = new DenseVector(velocity.toArray)
    val gBestPos = new DenseVector(gBest.position.toArray)
    val bestPos = new DenseVector(bestPosition.toArray)

    val newVel = W * vel + (Random.nextDouble() * c1 * (gBestPos - pos))
    val newPos = vel + pos

    this.copy(fit = newFit,
      bestFit = newBestFit,
      position = SVectors.dense(newPos.toArray),
      velocity = SVectors.dense(newVel.toArray),
      bestPosition = SVectors.dense(newBestPosition.toArray))
  }
}

object PSOParticle {
  def random(dimension: Int): PSOParticle = {
    new PSOParticle(dimension, Double.PositiveInfinity, Double.PositiveInfinity,
      SVectors.dense(Array.fill(dimension)(Random.nextDouble)),
      SVectors.dense(Array.fill(dimension)(Random.nextDouble)),
      SVectors.dense(Array.fill(dimension)(Random.nextDouble)))
  }

}

