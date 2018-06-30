package xajiusuo.ilikebannanas

import com.xajiusuo.job.config.{JobConfig, ParameterConfig, RuntimeConfig}
import com.xajiusuo.spark.SparkJobSubmits

/**
  * Created by Christopher Johnson on 2017/12/28.
  */

/*
* This file is used to configure the spark job of running the filter
* to create a reliable dataset for the linear regression model
*/
object Runspamfilter {
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
    runJobConfig.setQueue("TEMPORARYANALYSIS")
    val jobParamConfig = new ParameterConfig

    jobParamConfig.setParameter("impls", "xajiusuo.ilikebannanas.spamfilter")

    jobParamConfig.setParameter("input1.path", "hdfs://masterAB/user/hadoop/datas/dc/*/2016-06-12/*/*") //
    jobParamConfig.setParameter("input2.path", "hdfs://masterAB/user/hadoop/datas/dc/*/2016-06-13/*/*")
    jobParamConfig.setParameter("input3.path", "hdfs://masterAB/user/hadoop/datas/dc/*/2016-06-14/*/*")
    jobParamConfig.setParameter("input4.path", "hdfs://masterAB/user/hadoop/datas/dc/*/2016-06-15/*/*")
    jobParamConfig.setParameter("input5.path", "hdfs://masterAB/user/hadoop/datas/dc/*/2016-06-16/*/*")
    jobParamConfig.setParameter("output1.path", "hdfs://masterAB/tmp/yurboi-test/XD/pop_timedifftest3dasy")
    jobParamConfig.setParameter("output2.path", "hdfs://masterAB/tmp/yurboi-test/XD/TIMEDIFF2")


    jobConfig.setParameterConfig(jobParamConfig)
    jobConfig.setRuntimeConfig(runJobConfig)
    SparkJobSubmits.submit(jobConfig)
  }

}
