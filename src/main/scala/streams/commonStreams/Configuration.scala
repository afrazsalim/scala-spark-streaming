package streams.commonStreams

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import java.io.InputStream
import scala.util.{Failure, Success}
import scala.util.control.Exception.catching

object Configurator {

  private[this] def parseMap():Map[String,Map[String,String]] = {
    catching(classOf[Exception]).withTry({
    val stream: InputStream = getClass.getResourceAsStream("/settings.json")
    val json = scala.io.Source.fromInputStream( stream )
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue[Map[String,Map[String, String]]](json.reader())
    }) match {
      case Success(value) => value
      case Failure(exception) => throw new IllegalStateException(exception)
    }
  }

  def getConfigurations(settingType:String,settingName:String):String = {

    val parsedMap = parseMap()
    parsedMap.get(settingType) match {
      case Some(value) => value.get(settingName) match {
        case Some(res) => res
        case e => throw new IllegalStateException("Could not read json file. " + e)
      }
      case e => throw new IllegalStateException("Could not read json file. " + e)
    }
  }


}

