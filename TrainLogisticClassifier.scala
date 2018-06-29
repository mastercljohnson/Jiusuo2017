package xajiusuo.ilikebannanas

import com.xajiusuo.job.config.{JobConfig, ParameterConfig, RuntimeConfig}
import com.xajiusuo.spark.SparkJobSubmits

/**
  * Created by Christopher Johnson on 2018/1/3.
  */
object TrainLogisticClassifier {
  def main(args: Array[String]): Unit = {
    val jobConfig = new JobConfig
    val runJobConfig = new RuntimeConfig
    runJobConfig.setSpark_yarn_jar("hdfs://masterAB/user/spark/share/lib/spark-assembly-1.6.3-hadoop2.6.0.jar")
    runJobConfig.setExecutor_memory("15g")
    runJobConfig.setNum_executors("12")
    runJobConfig.setExecutor_cores("15")
    runJobConfig.setName("callerspam")
    runJobConfig.setSelf_jar("D:\\IdeaProjects\\jobsubmit\\classes\\artifacts\\jobsubmit_jar\\jobsubmit.jar")
    runJobConfig.setDepend_jars("D:\\IdeaProjects\\jobsubmit\\lib\\fastjson-1.2.5.jar," +
      "D:\\IdeaProjects\\jobsubmit\\lib\\positionarithemtic-1.0-SNAPSHOT.jar," +
      "D:\\IdeaProjects\\jobsubmit\\lib\\jts-1.8.jar," +
      "D:\\IdeaProjects\\jobsubmit\\lib\\job-impls123.jar")

    // Select hadoop cluster to train on
    runJobConfig.setQueue("TEMPORARYANALYSIS")
    val jobParamConfig = new ParameterConfig


    // Specify where to extract the training dataset
    jobParamConfig.setParameter("impls", "xajiusuo.ilikebannanas.classifier")

    jobParamConfig.setParameter("input1.path", "hdfs://masterAB/user/hadoop/tmp/yurboi-test/XD/pop_timedifftest3dasy") //
    jobParamConfig.setParameter("daystat.path", "hdfs://masterAB/user/hadoop/datas/dc/*/2016-06-13/*/*")

    jobParamConfig.setParameter("output1.path", "hdfs://masterAB/tmp/yurboi-test/XD/outputspam")
    jobParamConfig.setParameter("output2.path", "hdfs://masterAB/tmp/yurboi-test/XD/outputnonspam")



    jobConfig.setParameterConfig(jobParamConfig)
    jobConfig.setRuntimeConfig(runJobConfig)
    SparkJobSubmits.submit(jobConfig)
  }

}
