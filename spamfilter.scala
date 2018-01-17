package xajiusuo.ilikebannanas

import com.xajiusuo.job.AbstractJob
import com.xajiusuo.job.config.ParameterConfig
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by NS on 2017/12/28.
  */
class spamfilter extends AbstractJob {
  override def run(parameter: ParameterConfig): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val inputPath1 = parameter.getParameter("input1.path")
    val inputPath2 = parameter.getParameter("input2.path")
    val inputPath3 = parameter.getParameter("input3.path")
    val inputPath4 = parameter.getParameter("input4.path")
    val inputPath5 = parameter.getParameter("input5.path")
    val inputdials = parameter.getParameter("inputdialsonaday.path")
    val output1Path = parameter.getParameter("output1.path")
    val output2Path = parameter.getParameter("output2.path")

    //test
   // val test = getarray(inputPath1,output1Path,output2Path)
    //val justm = getarray(inputPath1)
    //justm.repartition(1).saveAsTextFile(output1Path)


    val daycomp = getarray(inputPath1).intersection(getarray(inputPath2)).intersection(getarray(inputPath3))
      //.intersection(getarray(inputPath4)).intersection(getarray(inputPath5))

    daycomp.repartition(1).saveAsTextFile(output1Path)
  }
  //val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val date0="2017-10-01 10:00:00"
//    System.currentTimeMillis()
//    sdf.format(System.currentTimeMillis())
//    sdf.parse(date0).getTime
//    sdf.parse(date0)

//    for(i<-0 until 30){
//      val date=sdf.format(sdf.parse(date0).getTime+i*86400000.toLong)
//      val file=this.sparkContext.textFile("////"+date+"*")
//    }
//    sdf.parse(date0).getTime

//  def pulldialpairs = {
//
//
//  }
//  def locationmatch(locations: String, dials: RDD[String]) ={
//  val locationlist = sparkContext.textFile(locations)
//
//
//
//
//


// method intersects multiple criterion for spam calls       , outputpath1: String, outputpath2: String
  def getarray(inputPath: String)={
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//  val inputPath = parameter.getParameter("input.path")
  //val outputPath = parameter.getParameter("output.path")

    val features = sparkContext.textFile(inputPath).persist()


//    val timesection = featuresprime.map(x => {
//      val cls = x.split("\t")
//      val m = sdf.parse(cls(8).substring(0,19)).getTime
//      (m , cls(1))
//    }).sortByKey().map(x => (x._2, x._1)).groupByKey(). map(x => {
//      val r = x._2.toArray.mkString("\t")
//      val r2 = r.split("\t")
//
//      val leng = r2(0).toLong +9*60*60*1000
//      //- r2(r2.length-1).toLong
//      val r3 = r2.flatten.filter(x=> x.toLong < leng)
//      (x._1, r3)
//    })



//    val cls = featuresprime.map(x=> {
//      val a = x.split("\t")
//      Array(a(1),a(8))
//    })

//    val features = featuresprime.filter(f => {
//      val cls = f.split("\t")
//
//      val rdd = sparkContext.parallelize((f(1),f(8)))
//
//      rdd.intersection(timesection2).count
      //cls.intersection(timesection2).count() != 0
//      val b = timesection.map()
//      cls.intersection(b)
//      timesection.intersection(cls).count()!= 0
      //m.
     // timesection.intersection(a
//   })

// something wrong with no previous part
//  val pickupline = features.filter(f => {
////// consistent call times
////// frequent calls in same place
//    val m = f.split("\t")
//      m.length> 9 && m(9).length>0 && m(1).length>0 && m.length> 7 && m(7).length>0 && m(5).length>0 && m(1).length>0
//    })
//      .map(x => {
//      val cls2 = x.split("\t")
//      //((cls2(5)+"\t"+cls2(7)+"\t"+cls2(1),1),cls2(9).toInt )
//        ((cls2(1),1),cls2(9).toInt)
//        //, cls2(5)+"\t"+cls2(7)))
//      })
//
//    val i = pickupline
//      .groupByKey()
//
//      val m = i.filter( x => {
//        val x2 = x._2
//        //val x2= x._2.toString().split(",")
//        val result = x2.aggregate((0,0))(
//        (acc, value) => (acc._1 + value, acc._2 + 1),
//        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
//      )
//      val avg = result._1 / result._2.toDouble
//      val m = x2.map(z => (z - avg) * (z - avg)).sum/x._2.size
//
//      m/avg <3
//        })
//         .map(x=> x._1).reduceByKey(_+_)
//        .filter(x=> x._2>10).map(x=>x._1)
        //        val cls = f.split("\t")
        //        cls(2)
        //      })
//        .filter(x => x._2 >50).map(x=> x._1+"\t"+x._2).map(f => {
//        val cls = f.split("\t")
//        cls(2)
//      })


// calls frequently
    val frequentboi = features.filter(f => {
      //// consistent call times
      //// frequent calls in same place
          val m = f.split("\t")
            m.length> 9 && m(9).length>0 && m(1).length>0 && m.length> 7 && m(7).length>0 && m(5).length>0 && m(1).length>0
          }).map(x => {
      val cls = x.split("\t")
      ((cls(5),cls(7),cls(1)),1)
    }).reduceByKey(_+_).filter(x=> x._2 > 100).map(f=> f._1._3)
// recieves calls more than threshhold
//    val tryflip = features.map(x => {
//  val cls = x.split("\t")
//  (cls(1),cls(9))
//}).invert

    val sopopular = features.filter(x => {
      val cls = x.split("\t")
      cls.length>10 && cls(10).length>0
    })
      .map(x=> {
      val cls = x.split("\t")
        (cls(10),1)
      })
      .reduceByKey(_+_).filter(f=> f._2>0)
      .map(x=> x._1)
//recieves calls less than threshhold
    val popularboi = features.filter(f => {
      // consistent call times
      val m = f.split("\t")
      m.length> 1 && m(1).length>0
    }).map(x=> {
      val cls = x.split("\t")
      cls(1)
    }).subtract(sopopular)
// calls a lot in short timespans
//something wrong with output
    val timesection = features.filter(x => {
      val cls = x.split("\t")
      cls.length>8 && cls(1).length>0 && cls(8).length>0
    }).map(x => {
      val cls = x.split("\t")
      val m = sdf.parse(cls(8).substring(0,19)).getTime
      (m , cls(1))
    }).sortByKey().map(x => (x._2, x._1))
      .groupByKey(). map(x => {
      val r = x._2.toArray

      val leng = r(0) +9*60*60*1000
      //- r2(r2.length-1).toLong
      val r3 = r.filter(x=> x < leng)
      var arr = new Array[Array[String]](r3.size)
      // var arr = List("m")
      val tarray = r3
      for (i<-0 to r3.size-1 ){
        arr(i) = new Array[String](r3.size)
        for (j<-1 to r3.size-1) {
          arr(i)(j) =(x._1+","+ r3(j).toString)

          //arr = x._1+","+ tarray(j).toString ::arr}
        }
      }
      arr.flatten.toString
      //(x._1, leng)
    })


    val timediff = features.filter(f => {
      // consistent call times
      val m = f.split("\t")
      m.length> 8 && m(8).length>0 && m(1).length>0
    }).map(x => {
      val cls = x.split("\t")
      val m = sdf.parse(cls(8).substring(0,19)).getTime
      ( cls(1), m)
    }).sortByKey()
      //.map(x => (x._2+","+ x._1))
//      .intersection(timesection).
//       .map(x => {
//      val m = x.split(",")
//      (m(0), m(1))
//    })
      .groupByKey().filter(x => {
      val r = x._2.toArray.mkString("\t")
      val r2 = r.split("\t")
      val arrayBuffer = new ArrayBuffer[Int]()
      for (n <- 0 to r2.size - 2){
        val count2 = 0
        var x = r2(n+1).toLong
        var y = r2(n).toLong
        if (x - y < 30){
          arrayBuffer+= 1
        }
      }
      val count = arrayBuffer.size
      count > 30
    }).map(f => f._1)















//
//    val onewayboi1 = features.map(f=>{
//      val cls = f.split("\t")
//      cls(1)})
//    val onewayboi = onewayboi1.subtract(onewayboi1.intersection(onewayboi1))
//
    //val finalscreen = popularboi.intersection(potato).intersection(pickupline).intersection(timediff)
    //val finalscreen = popularboi.intersection(timediff).intersection(m)
    //val finalscreen = popularboi.intersection(timediff).intersection(frequentboi)
    val finalscreen = popularboi.intersection(timediff)

    //val finalscreen = m
    finalscreen




    //test

//    timesection.repartition(1).saveAsTextFile(outputpath1)
//    timediff.repartition(1).saveAsTextFile(outputpath2)
////
//    finalscreen.repartition(1).saveAsTextFile(outputPath)
//    pickupline.repartition(1).saveAsTextFile(outputPath)
    //frequentboi.intersection(potato).repartition(1).saveAsTextFile(outputPath) .replaceAll("-","").replaceAll(" ","").replaceAll(":","")
  }
}
