package xajiusuo.ilikebannanas

import com.xajiusuo.job.AbstractJob
import com.xajiusuo.job.config.ParameterConfig

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Christopher Johnson on 2017/12/27.
  */

/**
  * Extract the phone information grouped by location
  * Under which signal towers
  */
class spatialcomp extends AbstractJob {

  // Datasets which positions are being read from
  override def run(parameter: ParameterConfig): Unit = {
    val inputPath1 = parameter.getParameter("input1.path")
    val inputPath2 = parameter.getParameter("input2.path")
    val outputPath1 = parameter.getParameter("output1.path")
    val outputPath2 = parameter.getParameter("output2.path")


    // Set variables of the datasets for manipulation
    val original1 = this.sparkContext.textFile(inputPath1)
    val original2 = this.sparkContext.textFile(inputPath2)

    // Order phone numbers by location (in this case coordinates of signal towers)

    // Reformat to extract only the location and phone numbers
    val parseforposition = original1.filter(x => {
      val cls = x.split("\t")
      cls.length>8 && cls(2).length>0 && cls(6).length>0 && cls(8).length>0
    })
      .map(y => {
        val cls = y.split("\t")
        (cls(5)+ "," + cls(7), cls(1))
      })

    // Group phone numbers by location
    val groupbylocation = parseforposition.groupByKey().map( x => x._2)

    val dataRDD2=original2.filter(x => {
      val cls = x.split("\t")
      cls.length>16 && cls(1).length>0 && cls(10).length>0
      //&& cls(15).length>0
    }).map(f=>{
      val cls=f.split("\t")
      if (cls(1)<cls(10)){
        (cls(1),cls(10))
      }else{
        (cls(10),cls(1))
      }
    }).distinct()

    //leftOuterJoin to reformat

    val extractpeople = groupbylocation
      .flatMap(  f   =>{
        var arr = new ArrayBuffer[(String,String)]()
        val t=f.toArray.mkString("\t")
        f.foreach(f1=>{
          arr+=((f1,t))
        })
        arr
      }).flatMap(f=>{
      val arr=new ArrayBuffer[String]()
      val cls=f._2.split("\t")
      cls.foreach(f1=>{
        if (!f._1.equals(f1))
          arr+=f._1+"\t"+f1
      })
      arr
    })

    // Find numbers within a certain vicinity

    val count = extractpeople.map(f=>{
      val cls=f.split("\t")
      if (cls(0)<cls(1)){
        (cls(0)+","+cls(1),1)
      }else{
        (cls(1)+","+cls(0),1)
      }
    }).reduceByKey(_+_)
    val count2 = count.map(x => x._1 +"\t"+ x._2)

    val expectclose = count2.filter( x => {
      val cls = x.split("\t")
      cls(2).toInt > 10
    } )

    // Save the regrouped phone numbers in a file
    expectclose.repartition(1).saveAsTextFile(outputPath1)
    dataRDD2.repartition(1).saveAsTextFile(outputPath2)


  }

}
