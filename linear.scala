package xajiusuo.ilikebannanas

import com.sun.rowset.internal.Row
import com.xajiusuo.job.AbstractJob
import com.xajiusuo.job.config.ParameterConfig
import org.apache.hadoop.hbase.client.Row
import org.apache.spark
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.regression
//import org.apache.spark.ml.stat.Correlation
//import org.apache.spark.sql.Row
/**
  * Created by NS on 2017/12/26.
  */
class linear extends AbstractJob{
  override def run(parameter: ParameterConfig): Unit = {
    val inputPath = parameter.getParameter("input.path")
    val outputPath = parameter.getParameter("output.path")
    val test1 = this.sparkContext.textFile(inputPath)

    val becomevector21 = test1.map(line => {
      val vec = line.split("\t")
      (vec(15))
    })
    val becomevector212 = test1.map(line => {
      val vec = line.split("\t")
      (vec(12))
    })
    val dataRDD=this.sparkContext.textFile(inputPath).filter(f=>{
      val cls=f.split("\t")
      cls.length>10 && cls(5).length>0 && cls(7).length>0 && cls(10).length>0
    }).map(f=>{
      val cls=f.split("\t")
      (cls(5)+","+cls(7),1)
    }).reduceByKey(_+_).map(f=>f._1+"\t"+f._2)
    dataRDD.repartition(1).saveAsTextFile(outputPath)
    // hdfs://masterAB/tmp/fd-test/Test/LacciCount
    //val n=0
    //val alpha1 =0
    /*val test2 = test1.reduceByKey((x,y) => {
      var step = x(n+1)-x(n)
      val alpha1 = alpha1 + 0.01*((y(n+1)-y(n)/(step)))*step
      val n = n + 1
    })
    */

   /*val df = test1.map(Tuple1.apply).toDF("features")
    val Row(coeff1: Matrix) =
      Correlation.corr(df, "features").head
    println("Pearson correlation matrix: \n" + coeff.toString)
    */



  }

}
