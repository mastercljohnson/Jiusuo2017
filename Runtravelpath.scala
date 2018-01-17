package xajiusuo.ilikebannanas

import com.xajiusuo.job.config.{JobConfig, ParameterConfig, RuntimeConfig}
import com.xajiusuo.spark.SparkJobSubmits

/**
  * Created by NS on 2017/12/26.
  */
object Runtravelpath {
  def main(args: Array[String]) {
    val jobConfig = new JobConfig
    val runJobConfig = new RuntimeConfig
    runJobConfig.setSpark_yarn_jar("hdfs://masterAB/user/spark/share/lib/spark-assembly-1.6.3-hadoop2.6.0.jar")
    runJobConfig.setExecutor_memory("10g")
    runJobConfig.setNum_executors("8")
    runJobConfig.setExecutor_cores("10")
    runJobConfig.setName("This will probably fail again")
    runJobConfig.setSelf_jar("D:\\IdeaProjects\\jobsubmit\\classes\\artifacts\\jobsubmit_jar\\jobsubmit.jar")
    runJobConfig.setDepend_jars("D:\\IdeaProjects\\jobsubmit\\lib\\fastjson-1.2.5.jar," +
      "D:\\IdeaProjects\\jobsubmit\\lib\\positionarithemtic-1.0-SNAPSHOT.jar," +
      "D:\\IdeaProjects\\jobsubmit\\lib\\jts-1.8.jar," +
      "D:\\IdeaProjects\\jobsubmit\\lib\\job-impls123.jar")
    runJobConfig.setQueue("TEMPORARYANALYSIS")
    val jobParamConfig = new ParameterConfig

    jobParamConfig.setParameter("impls", "xajiusuo.ilikebannanas.travelpath")

    jobParamConfig.setParameter("input.path", "hdfs://masterAB/user/hadoop/datas/dc/*/2017-12-22/*/*")//
    jobParamConfig.setParameter("output.path", "hdfs://masterAB/tmp/yurboi-test/Test/foreigntravel")

    jobConfig.setParameterConfig(jobParamConfig)
    jobConfig.setRuntimeConfig(runJobConfig)
    SparkJobSubmits.submit(jobConfig)
  }

}
