package streams.events
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import streams.commonStreams.PushStream
import scala.util.{Failure, Success}
import scala.util.control.Exception.catching

/**
 * An abstract class representing the Events.
 */
abstract class Events extends PushStream {

  /**
   * An inspector check if a specific event is in the list of allowed events.
   * @param currentEvents An array of current events that need to be checked.
   * @param allowedEvents An array of allowed event.
   * @return True if and onyl if a specific event occurs in the list of allowed events.
   */
  protected [this] def isValidEvent(currentEvents:Array[String], allowedEvents:Array[String]):Boolean = {
      if(allowedEvents == null)
         throw new IllegalStateException("Allowed events cannot be null")
      assert(allowedEvents.length > 0)
      if(currentEvents == null) {
         return true
      }
    for(elem <- currentEvents){
         if(allowedEvents.contains(elem) || elem=="APP")
           return true
       }
    false
  }

  /**
   * Reads a data from kafka and perform some common operation on the data.
   * @param pushEventInstance instance of the stream.
   * @return dataframe containing the data from kafka.
   */
  def applyCommonEventsOperations(pushEventInstance:PushStream):DataFrame = {
    registerStreamListener(getSparkSession(pushEventInstance), pushEventInstance)
    val rawDF = getRawDFfromSession()
    val bronzeDF = filterOnTime(filterOnMac(rawDF),"event_time")
    val silverDF = addDateAndHourToJsonFromEpochField(bronzeDF,"event_time",getDateFrom,getHourFrom)
    val goldDF1 = addDfColumnValueToJSONString(silverDF,addColumnValueToJson,"event_date")
    val finalDF = removeIllegalChars(goldDF1,removeIllegalCharacters)
    finalDF
  }


  override def refineDF(silverDF:DataFrame):DataFrame = {
        silverDF.select("events.*","event_date","logstash_processed_at_date","logstash_processed_at")
                .withColumn("event_time",updateEpochField(col("event_time")))

  }



  /**
   * Read the allowed events from file and only these events will be written to the disk.
   * @param path Path to the file containing allowed events.
   * @param pushEventInstance An instance of the pushEvents stream.
   * @return An array of string containing the events that are allowed.
   */
  protected [this] def getAllowedEventsFromFile(path:String,pushEventInstance:PushStream):Array[String] = {
      catching(classOf[Exception]).withTry(getSparkSession(pushEventInstance).read.csv(path).rdd.collect().toList.head.toString().split(",")) match {
        case Success(value) =>
          println("Successfully read the events: Following events are allowed. " + value.toList.foldLeft(""){case (acc,elem) => acc+ " " + elem})
          value
        case Failure(exception) => throw  new IllegalStateException(exception)
      }
  }

}
