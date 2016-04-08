package com.aksw.spark
import org.apache.spark._
import org.apache.spark.SparkContext

object PsoSpark {
  
  val dim: Int = 4
  var gbest: PSOParticle = new PSOParticle()
  var tbest: PSOParticle = new PSOParticle()

  
  

  class PSOParticle() {
    //FlatMapFunction[(PSOParticle), (PSOParticle) ]{
    var dimension: Int = dim
    var pfit: Double = 100000.0
    var fit: Double = 10000000.0
    var position: Array[Double] = (1 to dim toArray) map (x => Math.random())
    var velocity: Array[Double] = (1 to dim toArray) map (x => Math.random())
    var pbest: Array[Double] = (1 to dim toArray) map (x => Math.random())
    var sum =0.0
    def Copy(particle: PSOParticle): Unit = {

      0 until particle.dimension foreach { a =>
        this.velocity(a) = particle.velocity(a)
        this.position(a) = particle.position(a)
      }
      this.fit = particle.fit

    }

    def stringString(): String = {
      var st = new StringBuilder()
      0 until this.dimension foreach { a =>
        st.append("\n Vel")
        st.append(this.velocity(a).toString)
        st.append("\n Pos")
        st.append(this.position(a).toString)
      }
      st.append("\n \n--Fitness =")
      st.append(this.fit.toString)
      println("To String" + st)
      var temp = new String()
      temp.addString(st)
      return temp
    } 
    def sphere ( ): Unit =
        {
          this.fit =0.0
          this.sum =0.0
          0 until this.dimension foreach { j =>

            this.sum = this.sum + this.position(j) * this.position(j)
          }
          this.fit=this.sum
          if(this.pfit>this.sum) {
            this.pfit = this.sum
            this.position.copyToArray(this.pbest)
          }

        }
    def updateP(particle: PSOParticle) {
            val c1 = 1.49618
            val c2 = 1.49618
            val W =  0.7298

          0 until this.dimension foreach { a =>

            this.velocity(a) = W * this.velocity(a) + (Math.random() * c1 * (gbest.position(a) - this.position(a))) + (Math.random() * c2 * (this.pbest(a) - this.position(a)))
            this.position(a) = this.velocity(a) + this.position(a)


          }
    
  }
  }

  //override def Map ()

  
  
  def main(args: Array[String]): Unit = {
      // execution parameters
    val iter = 3
    val populationsize = 5
    val size = populationsize
  val population = Seq((1 to size) map(x => new PSOParticle()))

  
     val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
     val sc = new SparkContext(conf)
    val distData = sc.parallelize(population)
//population.map { x => x.span(x.sphere) }
//    
//    population.map(x => sphere())
//    0 until iter foreach { j =>
//      population.map(x => sphere(_))
//      population.map(x => updateP(_)) // NOT BEING UPDATED
      
      // population.groupBy(pop[1].fit).sum("fit").print()
//      println("........................................................." + gbest.stringString())
//
//    }

   }
  
}