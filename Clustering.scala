package xajiusuo.ilikebannanas

import com.xajiusuo.job.AbstractJob
import com.xajiusuo.job.config.ParameterConfig
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark

/**
  * Created by Christopher Johnson on 2017/12/26.
  */
class Clustering extends AbstractJob {
  override def run(parameter: ParameterConfig): Unit = {
    val inputPath = parameter.getParameter("input.path")
    val outputPath = parameter.getParameter("output.path")

    // Format the datastructures from the csv files in hadoop clusters

    val datardd1 = this.sparkContext.textFile(inputPath).map( x => {
      var cls = x.split("\t")
      cls.length > 15 && cls(15) !=0 && cls(2).length> 0
      && cls(10).length > 0 && cls(9).length > 0
      (cls(2)+","+cls(10),cls(9))
    })


    val datardd2 = this.sparkContext.textFile(inputPath).map( x => {
      var cls = x.split("\t")
      cls.length > 15 && cls(15) !=0 && cls(2).length> 0
      && cls(10).length > 0 && cls(9).length > 0
      (cls(10)+","+cls(2),cls(9))
    })


    val datardd3 = datardd2.intersection(datardd1)

    val datardd4 = datardd3.union(datardd2.subtract(datardd3))

    val datardd5 = datardd4.map(f => f._1 +"\t"+ f._2)

    // Using Kmeans clustering to find out where the highest amount of calls
    // Came from
/*
    val parsedData = datardd5.map( s => Vectors.dense(s.split("\t").map(_.toDouble))).cache()

     val numClusters = 5
     val numIterations = 40

     val clusters:KMeansModel = KMeans.train(parsedData, numClusters, numIterations)
     val WSSSE = clusters.computeCost(parsedData)

     println("Square Error =" + WSSSE)

*/

  }

}
