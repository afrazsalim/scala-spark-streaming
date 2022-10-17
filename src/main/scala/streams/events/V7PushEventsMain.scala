package streams.events
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import streams.commonStreams.UDFUtil

object V7PushEventsMain extends Events {


  def main(args: Array[String]): Unit = {
    super.setArguments(args,"kafka_to_hive_v7_events",Seq("event_date","event_type"))
    super.initUDFs()
    val goldDF = super.applyCommonEventsOperations(this)
                      .filter(row => UDFUtil.existField(row.get(0).toString, "event_type"))
                      .withColumn("value", replaceValueInJson(col("value"), lit("hw_type"), lit("v7")))
    super.writeToHDFS(goldDF)
  }





  override def refineDF(silverDF: DataFrame): DataFrame = {
    var tempDF:DataFrame= super.refineDF(silverDF)
      .drop("events")
      .drop("http_type")
      .drop("hdcp_status")
      .drop("last_known_heart_beat")
      .withColumn("mac",col("mac_address"))
    if(tempDF.columns.contains("scan_start_time")) {
      tempDF = tempDF.withColumn("scan_start_time",col("scan_start_time").cast("decimal(18,0)"))
    }
    if(tempDF.columns.contains("event_time")) {
      tempDF = tempDF.withColumn("event_time",col("event_time").cast("decimal(18,0)"))
    }
    if(tempDF.columns.contains("eventcounter")) {
      tempDF = tempDF.withColumn("eventcounter",col("eventcounter").cast("decimal(18,0)"))
    }
    if(tempDF.columns.contains("event_counter")) {
      tempDF = tempDF.withColumn("event_counter",col("event_counter").cast("decimal(18,0)"))
    }
    if(tempDF.columns.contains("viewing_duration")) {
      tempDF = tempDF.withColumn("viewing_duration",col("viewing_duration").cast("decimal(18,0)"))
    }
    if(tempDF.columns.contains("program_duration")) {
      tempDF = tempDF.withColumn("program_duration",col("program_duration").cast("decimal(18,0)"))
    }
    if(tempDF.columns.contains("time_position")) {
      tempDF = tempDF.withColumn("time_position",col("time_position").cast("decimal(18,0)"))
    }
    if(tempDF.columns.contains("event_time")) {
      tempDF = tempDF.withColumn("event_time",col("event_time").cast("decimal(18,0)"))
    }
    if(tempDF.columns.contains("app_duration")) {
      tempDF = tempDF.withColumn("app_duration",col("app_duration").cast("decimal(18,0)"))
    }
    tempDF
  }
}
