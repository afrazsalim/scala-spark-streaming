package streams.metrics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit}


object V7PushMetricsMain extends Metrics {

  override def refineDF(silverDF:DataFrame):DataFrame = {
      super.refineDF(silverDF)
              .withColumn("mac",col("mac_address"))
     }


  def main(args: Array[String]): Unit = {
      super.setArguments(args,"kafka_to_hive_v7_metrics",Seq("thedate"))
      super.initUDFs()
      val goldDF = super.applyCommonMetricsOperations(this)
                        .withColumn("value", replaceValueInJson(col("value"), lit("hw_type"), lit("v7")))
      super.writeToHDFS(goldDF.drop("epoch_time"))
   }


}
