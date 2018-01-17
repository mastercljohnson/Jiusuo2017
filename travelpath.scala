package xajiusuo.ilikebannanas

import com.xajiusuo.job.AbstractJob
import com.xajiusuo.job.config.ParameterConfig
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.NonFatal

/**
  * Created by NS on 2017/12/26.
  */
class travelpath extends AbstractJob {
  override def run(parameter: ParameterConfig): Unit = {
    //val sparkConf = new SparkConf().setAppName("test").setMaster("local[4]")
    //val sc = new SparkContext(sparkConf)
    val inputPath = parameter.getParameter("input.path")
    val outputPath = parameter.getParameter("output.path")
    val data = this.sparkContext.textFile(inputPath).map( x=> {
      val vector = x.split("\t")
      println(x.length)
      try {
        (vector(16) + "," + vector(4) + "," + vector(5), 1)
      }
      catch {
        case NonFatal(ex) => (vector(4)+","+vector(5),1)
      }
    })
      val firstout = data.reduceByKey(_+_).map(x => x._1 +","+x._2)
    firstout.repartition(1).saveAsTextFile(outputPath)




  }

}
