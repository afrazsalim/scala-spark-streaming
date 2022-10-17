package streams.events
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

object V6PushEventsMain extends Events {


  def main(args: Array[String]): Unit = {
    super.setArguments(args,"kafka_to_hive_v6_events",Seq("event_date","event_type"))
    super.initUDFs()
    val goldDF = super.applyCommonEventsOperations(this)
                       .withColumn("value", replaceValueInJson(col("value"), lit("hw_type"), lit("v6")))
    super.writeToHDFS(goldDF)
  }

  override def refineDF(silverDF: DataFrame): DataFrame = {
    super.refineDF(silverDF)
      .drop("events")
      .drop("http_type").withColumn("mac",col("mac_address"))
      .withColumn("event_type",col("sub_type"))
      .filter(col("event_type").isNotNull)
      .filter(!(col("event_type").contains("%")))
      .filter(!col("event_type").contains("?"))
      .withColumn("logstash_date",col("event_date"))
      .withColumn("event_epoch_time",col("event_time"))
  }


}
