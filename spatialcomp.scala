package xajiusuo.ilikebannanas

import com.xajiusuo.job.AbstractJob
import com.xajiusuo.job.config.ParameterConfig

import scala.collection.mutable.ArrayBuffer

/**
  * Created by NS on 2017/12/27.
  */
class spatialcomp extends AbstractJob {
  override def run(parameter: ParameterConfig): Unit = {
    val inputPath1 = parameter.getParameter("input1.path")
    val inputPath2 = parameter.getParameter("input2.path")
    val outputPath1 = parameter.getParameter("output1.path")
    val outputPath2 = parameter.getParameter("output2.path")

    val original1 = this.sparkContext.textFile(inputPath1)
    val original2 = this.sparkContext.textFile(inputPath2)

    val parseforposition = original1.filter(x => {
      val cls = x.split("\t")
      cls.length>8 && cls(2).length>0 && cls(6).length>0 && cls(8).length>0
    })
      .map(y => {
        val cls = y.split("\t")
        (cls(5)+ "," + cls(7), cls(1))
      })
    val groupbylocation = parseforposition.groupByKey().map( x => x._2)

/*    val groupbycall1 = original2.filter(x => {
      val cls = x.split("\t")
      cls.length>16 && cls(2).length>0 && cls(10).length>0
      //&& cls(15).length>0
    })
      .map(y => {
        val cls = y.split("\t")
        (cls(4), cls(2))
      })

    val groupbycall2 = original2.filter(x => {
      val cls = x.split("\t")
      cls.length>16 && cls(2).length>0 && cls(10).length>0
      //&& cls(15).length>0
    })
      .map(y => {
        val cls = y.split("\t")
        (cls(2), cls(4))
      })
*/
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

    //leftOuterJoin

/*    val groupbycall3 = groupbycall1.intersection(groupbycall2)
    val groupbycall4 = groupbycall3.union(groupbycall1.subtract(groupbycall3)).map( x => x._1 +"\t"+ x._2).cache()
*/

//    val extractpeople = groupbylocation.cartesian(groupbylocation).map( x=> (x._1 +"\t"+ x._2))
//    val extractpeople = groupbylocation.flatMap(f => {
//      var arr = new ArrayBuffer[(String,String)]()
//`     val tarray = f.toArray
//      for(i<-0 to tarray.size-1){
//        for(j<-i+1 to tarray.size-1){
//          arr+=((tarray(i),tarray(j)))
//        }
//      }
//    arr
//    })
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
//  .flatMap(f => {
//      var arr = new ArrayBuffer[(String,String)]()
//      val tarray = f.toArray
//      for(i<-0 to tarray.size-1){
//        for(j<-0 to tarray.size-1){
//          arr +=((tarray(i),tarray(j)))
//        }
//      }
//      arr
//    }).map( x=> (x._1 +"\t"+ x._2))




/*    val extractpeople2 = groupbylocation.cartesian(groupbylocation).map( x=> (x._2, x._1))
    val extractppl3 = extractpeople.intersection(extractpeople2)
    val extractppl4 = extractppl3.union(extractpeople.subtract(extractppl3))

    val count = extractppl4.map( x => (x._1+","+x._2, 1)).reduceByKey(_+_)
*/
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


 //   val extractcount = count.filter( x => {
 //     x._2 > 3}).map(x=> x._1)

   /* val callfilter = groupbycall4.map(x => {
      val cls = x.split("\t")
      (cls(1),cls(2))
    })*/



    expectclose.repartition(1).saveAsTextFile(outputPath1)
    dataRDD2.repartition(1).saveAsTextFile(outputPath2)


  }

}
