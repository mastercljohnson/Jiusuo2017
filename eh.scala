package xajiusuo.ilikebannanas

import com.xajiusuo.job.AbstractJob
import com.xajiusuo.job.config.ParameterConfig
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by NS on 2018/1/2.
  */
class eh extends AbstractJob{
  override def run(parameter: ParameterConfig): Unit = {
    val inputPath1 = parameter.getParameter("input1.path")
    //val inputPath2 = parameter.getParameter("input2.path")
    val inputPathday = parameter.getParameter("daystat.path")
    val outputPath1 = parameter.getParameter("output1.path")
    val outputPath2 = parameter.getParameter("output2.path")


    val spamnumbers = this.sparkContext.textFile(inputPath1)
    //val nonspamnumbers = this.sparkContext.textFile(inputPath2)
    val day = this.sparkContext.textFile(inputPathday)
//spam vector create
    val spamvectorextractcallfromlength = day.filter(x => {
      val cls = x.split("\t")
      cls.length> 10 && cls(9).length>0 && cls(1).length>0
    }).map( x=> {
      val cls = x.split("\t")
      (cls(1),cls(9).toInt)
          }). reduceByKey(_+_)

    val spamvectorsextractcallfromamount = day.filter(x => {
      val cls = x.split("\t")
      cls.length> 10 && cls(9).length>0 && cls(1).length>0
    }).map( x=> {
      val cls = x.split("\t")
      (cls(1),1)
    }). reduceByKey(_+_)

    val spamvectorsextractcallto = day.filter(x => {
      val cls = x.split("\t")
      cls.length> 10 && cls(1).length>0 && cls(10).length>0
    }).map( x=> {
      val cls = x.split("\t")
      (cls(10),1)
    }).reduceByKey(_+_)

    val spamexist = spamnumbers.map(x=> (x, 1))

    val realspamvector = spamvectorextractcallfromlength.union(spamvectorsextractcallfromamount).union(spamvectorextractcallfromlength)
        .union(spamexist).groupByKey().filter(x=> x._2.toArray.length >3 ).map(x=> x._1+","+x._2.toString()).map(x=> {
      val cls = x.split(",")
      cls(0).charAt(0)+"\t"+cls(1)+"\t"+cls(2)+"\t"+cls(3)
    })

    //nonspam

    val nonspamnumbers = day.filter(x=> {
      val cls = x.split("\t")
      cls.length>0 && cls(1).length>0
    })
      .map(x=> {
        val cls = x.split("\t")
        cls(1)}).subtract(spamnumbers)

    val nonspamexist = nonspamnumbers.map(x=>(x,1))

    val realnonspamvector = spamvectorextractcallfromlength.union(spamvectorsextractcallfromamount).union(spamvectorextractcallfromlength)
      .union(nonspamexist).groupByKey().filter(x=> x._2.toArray.length >3 ).map(x=> x._1+","+x._2.toString()).map(x=> {
      val cls = x.split(",")
      cls(0).charAt(0)+"\t"+cls(1)+"\t"+cls(2)+"\t"+cls(3)
    })

    val tf = new HashingTF(4)

    val spamfeat = realspamvector.map(x=> tf.transform(x.split(",")))
    val nonfeat = realnonspamvector.map(x=> tf.transform(x.split(",")))

    val labelspam = spamfeat.map(f => LabeledPoint(1,f))
    val labelnon = nonfeat.map(f => LabeledPoint(0,f))
    val traindat = labelspam.union(labelnon)
    traindat.cache()

    val model = new LogisticRegressionWithSGD().run(traindat)

    val test = tf.transform(("1,1,1,3").split(","))

    println(model.predict(test))

//    realspamvector.repartition(1).saveAsTextFile(outputPath1)
//    realnonspamvector.repartition(1).saveAsTextFile(outputPath2)
  }
}
