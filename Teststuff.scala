package xajiusuo.ilikebannanas

import com.xajiusuo.job.AbstractJob
import com.xajiusuo.job.config.ParameterConfig

/**
  * Created by Christopher Johnson on 2017/12/27.
  */
class Teststuff extends AbstractJob{
  override def run(parameter: ParameterConfig): Unit = {
    val weektrain = parameter.getParameter("weektrain.path")
    val weekcross = parameter.getParameter("weekcross.path")
    val modelout = parameter.getParameter("modelout.path")

    val datardd = this.sparkContext.textFile(weektrain)
    val crossrdd = this.sparkContext.textFile(weekcross)



  }

}
