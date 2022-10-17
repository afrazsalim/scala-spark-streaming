package streams.metrics

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, concat, lit, udf}
import streams.commonStreams.PushStream


object V6PushMetricsMain extends Metrics {



  override def refineDF(silverDF:DataFrame):DataFrame = {
    super.refineDF(silverDF)
            .withColumn("mac",col("mac_address"))
  }


  def main(args: Array[String]): Unit = {
    super.setArguments(args,"kafka_to_hive_v6_metrics",Seq("thedate"))
    super.initUDFs()
    val goldDF = super.applyCommonMetricsOperations(this)
                      .withColumn("value", replaceValueInJson(col("value"), lit("hw_type"), lit("V6")))
    super.writeToHDFS(goldDF.drop("epoch_time"))
  }


}
