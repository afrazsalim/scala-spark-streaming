package streams.metrics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import streams.commonStreams.PushStream

/**
 * An abstract class the represents the metrics.
 */
abstract class Metrics extends PushStream{


  override def refineDF(silverDF:DataFrame):DataFrame = {
      silverDF.select("metrics.*","thedate")
              .withColumn("epoch_time",updateEpochField(col("epoch_time")))
  }

  /**
   * Read data from kafka and apply some common metrics operations on that data.
   * @param pushMetricsInstance instance of metrics.
   * @return a dataframe containing data from kafka.
   */
  def applyCommonMetricsOperations(pushMetricsInstance:PushStream):DataFrame = {
    registerStreamListener(getSparkSession(pushMetricsInstance), pushMetricsInstance)
    val rawDF = getRawDFfromSession()
    val bronzeDF = filterOnTime(filterOnMac(rawDF),"epoch_time")
    val silverDF = addDateAndHourToJsonFromEpochField(bronzeDF,"epoch_time",getDateFrom,getHourFrom)
    val goldDF1 = addDfColumnValueToJSONString(silverDF,addColumnValueToJson,"thedate")
    val finalDF = removeIllegalChars(goldDF1,removeIllegalCharacters) //Rename the function.
    finalDF
  }




}
