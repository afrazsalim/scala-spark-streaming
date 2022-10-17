package streams.commonStreams
import org.json4s.jackson.JsonMethods.parse

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.util.{Failure, Success}
import scala.util.control.Exception.catching


object UDFUtil {

  private[this] def findValueFromInnerMap(input: Map[String, Any], query: String): String = {
    for ((key, value) <- input) {
      if (value.isInstanceOf[Map[String, Any]]) {
        return getValueFromMap(input, query)
      }
      else if (key == query)
        return value.toString
    }
    null
  }


  private[this] def containsKeyValues(input: String, query: String): Boolean = {
    catching(classOf[Exception]).withTry(parse(input).values.asInstanceOf[Map[String, Any]]) match {
      case Success(value) => catching(classOf[Exception]).withTry({getValueFromMap(value, query)}) match {
        case Success(output:String) =>
          if(query=="event_time"  || query == "epoch_time") {
            (output != null) && (output forall Character.isDigit)
          }else{
            output != null
          }
        case Failure(_) => false
      }
      case Failure(_) => throw new IllegalStateException("Could not parse the json string")
    }
  }

  /**
   * A getter to get the value from a nested map.
   * @param input The nested map.
   * @param query key for which a value needs to be found.
   * @return A value associated with given key in string format.
   */
  def getValueFromMap(input: Map[String, Any], query: String): String = {
    for ((key, value) <- input) {
      value match {
        case map: Map[String, Any] =>
          val result = findValueFromInnerMap(map, query)
          if (result != null) {
            return result
          }
        case _ => if (key == query)
          return value.toString
      }
    }
    null
  }


  /**
   * An inspector to check if the key and value associated with that key are present in a JSON string.
   * @param inputJson An input JSON string.
   * @param key The key to be checked.
   * @return True if the key and value associated with that key exist in JSON string.
   */
  def existField(inputJson: String, key: String): Boolean = {
    catching(classOf[Exception]).withTry(containsKeyValues(inputJson, key)) match {
      case Success(result) => result
      case Failure(_) => false
    }
  }



  def isValidDate(dateValue:String):Boolean = {
    val dummyDate:String = "2010-01-01 00:00:00"
    if(dateValue ==null)
      return false
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val currentDate:String = dateFormat.format(Calendar.getInstance().getTime())
    dateFormat.parse(dateValue).compareTo(dateFormat.parse(currentDate))<= 0 && dateFormat.parse(dateValue).compareTo(dateFormat.parse(dummyDate))>=0
  }


}
