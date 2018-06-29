package xajiusuo.ilikebannanas

import com.xajiusuo.job.AbstractJob
import com.xajiusuo.job.config.ParameterConfig
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by Christopher Johnson on 2018/1/2.
  */

/**
  * This file was used to train the dataset which was obtained by the
  * previous filter on a linear regression model
  */
class eh extends AbstractJob{

  // Configure the inputs for the datasets to train on
  // as well as the output for the model weights
  override def run(parameter: ParameterConfig): Unit = {
    val inputPath1 = parameter.getParameter("input1.path")
    //val inputPath2 = parameter.getParameter("input2.path")
    val inputPathday = parameter.getParameter("daystat.path")
    val outputPath1 = parameter.getParameter("output1.path")
    val outputPath2 = parameter.getParameter("output2.path")

    // Set variables for the spam numbers and the time interval
    // which are used for the training
    val spamnumbers = this.sparkContext.textFile(inputPath1)
    val day = this.sparkContext.textFile(inputPathday)

    // Format the spam caller dataset to be fed into the linear regression model

    // Extract the length of the call as one of the parameters
    val spamvectorextractcallfromlength = day.filter(x => {
      val cls = x.split("\t")
      cls.length> 10 && cls(9).length>0 && cls(1).length>0
    }).map( x=> {
      val cls = x.split("\t")
      (cls(1),cls(9).toInt)
          }). reduceByKey(_+_)

    // Extract the amount of calls of each number as a parameter
    val spamvectorsextractcallfromamount = day.filter(x => {
      val cls = x.split("\t")
      cls.length> 10 && cls(9).length>0 && cls(1).length>0
    }).map( x=> {
      val cls = x.split("\t")
      (cls(1),1)
    }). reduceByKey(_+_)

    // Extract the recievers of said caller as a parameter
    val spamvectorsextractcallto = day.filter(x => {
      val cls = x.split("\t")
      cls.length> 10 && cls(1).length>0 && cls(10).length>0
    }).map( x=> {
      val cls = x.split("\t")
      (cls(10),1)
    }).reduceByKey(_+_)

    // Format so that the value of the output would be 1 for numbers
    // which are in the spam category in the training set
    val spamexist = spamnumbers.map(x=> (x, 1))

    // Perform set operations to obtain correct training set from the
    // Extracted features indicated above (call length, frequencies,
    //                                      dialed number )
    val realspamvector = spamvectorextractcallfromlength
                          .union(spamvectorsextractcallfromamount)
                          .union(spamvectorextractcallfromlength)
                          .union(spamexist)
                          .groupByKey()
                          .filter(x=> x._2.toArray.length >3 )
                          .map(x=> x._1+","+x._2.toString())
                          .map(x=> {
      val cls = x.split(",")
      cls(0).charAt(0)+"\t"+cls(1)+"\t"+cls(2)+"\t"+cls(3)
                                    })

    // Likewise, perform a similar extraction process for the non-spam caller
    // dataset

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


    // Set up hashing for dataset used to train logistic regression
    val tf = new HashingTF(4)


    // Format vectors and parameters to be fed into the logistic regression
    // classifiers
    val spamfeat = realspamvector.map(x=> tf.transform(x.split(",")))
    val nonfeat = realnonspamvector.map(x=> tf.transform(x.split(",")))

    // Set the spam data giving the output 1 and the non spam data giving 0
    val labelspam = spamfeat.map(f => LabeledPoint(1,f))
    val labelnon = nonfeat.map(f => LabeledPoint(0,f))
    val traindat = labelspam.union(labelnon)

    // Save the dataset for training
    traindat.cache()

    // Create a logistic regression model from the Apache Spark MLLib library
    // and use the dataset from above for training
    val model = new LogisticRegressionWithSGD().run(traindat)

    // Test on a trivial example
    val test = tf.transform(("1,1,1,3").split(","))

    // Print the result of the training on this piece of trivial data
    println(model.predict(test))

    // Save the outputs for the weights in the classifier
//    realspamvector.repartition(1).saveAsTextFile(outputPath1)
//    realnonspamvector.repartition(1).saveAsTextFile(outputPath2)
  }
}
