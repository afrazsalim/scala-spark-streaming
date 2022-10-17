package streams.commonStreams
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.json4s.jackson.JsonMethods.parse
import streams.commonStreams.ProcessDataForHive.{meteDataMap, removeMetaDataFile, shouldRemoveMetadataFile}
import java.net.URI
import java.text.SimpleDateFormat
import javax.naming.InsufficientResourcesException
import scala.util.control.Exception.catching
import scala.util.{Failure, Success}
import scala.util.Try
/**
 * PushStream is a trait and each metrics stream or events stream derives from PushStream trait. PushStream extends Serializeable trait and hence, any class
 * extending PushStream is also Serializable.
 */
@SerialVersionUID(100L)
trait PushStream extends Serializable {


  private[this] var _startingOffsets: String = _
  private[this] var _hivetableName: String = _
  private[this] var _checkpointLocation: String = _
  private[this] var _outputPath: String = _
  private[this] var _hiveOutputPath: String = _
  private[this] var _topic: String = _
  private[this] var _logLevel: String = _
  private[this] var _maxOffSetsPerTrigger: String = _
  private[this] var _minOffsetsPerTrigger: String = _
  private[this] var _pollTime: String = _
  private[this] var _triggerDuration: String = _
  private[this] var _nameOfStream: String = _
  private[this] var partitionColsForHive:Seq[String] = _
  private[this] var _maxTriggerDelay: String = _
  private[this] var cleanStart:String = _
  private[this] var _confDirectory: String = _

  protected def getConfDirectory: String = _confDirectory

  private[this] def confDirectory_(value: String): Unit = {
    _confDirectory = value
  }

  //private[this] val KAFKA_BOOTSTRAP_SEVER = "localhost:9092"
  //"kafka.bootstrap.servers":"el5185.bc:6667,el5186.bc:6667,el5187.bc:6667,el5188.bc:6667",

  val HDFS_BASE_PATH  = Configurator.getConfigurations("hdfs_settings","hdfs_uri")
  //val HDFS_BASE_PATH = "hdfs://localhost:9000"

//"org.apache.spark.sql.kafka010.KafkaSourceProvider"


  var updateEpochField: UserDefinedFunction = _

  /**
   * Takes a json string, a key and a value. It replaces the value in input JSON string corresponding to the give key, with the give value.
   */
  var replaceValueInJson: UserDefinedFunction = _
  /**
   * It takes a JSON string, a key and a value and adds the key-value pair to the JSON string.
   */
  var addColumnValueToJson: UserDefinedFunction = _
  /**
   * Takes a JSON string, source key and a target key.
   * It extracts the value associated with source key, from the JSON string and inserts the value with the target key in the JSON string.
   */
  var addFields: UserDefinedFunction = _
  /**
   * It takes a JSON string and the field name that indicates the date. It extracts the date from the JSON and returns as a string.
   */
  var getDateFrom: UserDefinedFunction = _
  /**
   *It takes a JSON string and a field indicating the time. It returns the hour extracted from the JSON.
   */
  var getHourFrom: UserDefinedFunction = _
  /**
   * It takes a JSON string and removes the illegal characters fromt the JSON string.
   */
  var removeIllegalCharacters: UserDefinedFunction = _

  var hasCol: UserDefinedFunction = _


  /**
   * A getter to get the offset configuration which are passed through the terminal.
   * @return the starting offfsets passed via terminal.
   */
  def getStartingOffsets: String = _startingOffsets

  /**
   * A getter to get the checkPointLocation,extended with HDFS URI.
   * @return the absolute checkPointLocation.
   */
  def getCheckpointLocation: String = HDFS_BASE_PATH+_checkpointLocation

  /**
   * A getter to get the streams outputpath.
   * @return the output path where stream from KAFKA stores the data.
   */
  def getStreamOutputPath: String = HDFS_BASE_PATH+_outputPath

  /**
   * A getter to get the hive output path.
   * @return The absolute path where data for hive is stored.
   */
  def getHiveOutputPath: String = HDFS_BASE_PATH+_hiveOutputPath

  /**
   * A getter to get the KAFKA topic from which data will be read.
   * @return the kafka topic from where the data is read.
   */
  def getTopic: String = _topic

  /**
   * A getter to get the logLevel of spark application.
   * @return the logLevel of spark application, which is passed via terminal.
   */
  def getLogLevel: String = _logLevel

  /**
   * A getter to get the the kafka configuration of maxOffsetsPerTrigger.
   * @return the configuration of kafka to get maxOffsetPerTrigger.
   */
  def getMaxOffSetsPerTrigger: String = _maxOffSetsPerTrigger

  /**
   * A getter to get the kafka configuration of minOffsetsPerTrigger.
   * @return the configuration of kafka to get minOffsetsPerTrigger.
   */
  def getMinOffsetsPerTrigger: String = _minOffsetsPerTrigger

  /**
   * A getter to get kafka configuration of pollTime.
   * @return the pollTime that is passed via terminal.
   */
  def getPollTime: String = _pollTime

  /**
   * A getter to get the kafka configuration of triggerDuration.
   * @return the triggerDuration for kafka stream.
   */
  def getTriggerDuration: String = _triggerDuration

  /**
   * A getter to get the hiveTableName.
   * @return the hiveTableName that is passed via terminal.
   */
  def getHivetableName: String = _hivetableName

  /**
   * A getter to get the name of the stream.
   * @return the name of the stream.
   */
  def getNameOfStream:String = _nameOfStream

  /**
   * A getter to get the partitioning columns for hive.
   * @return the partitioning columns for hive.
   */
  def getPartitionColsForHive:Seq[String] = partitionColsForHive

  /**
   * A getter to get the maxTriggerDelay of kafka configuration.
   * @return the maxTriggerDelay of kafka stream.
   */
  def getMaxTriggerDelay: String = _maxTriggerDelay


  def shouldStartFromBeginning(): Boolean = cleanStart == "true"

  private [this] def setPartitionColsForHive(inputData:Seq[String]): Unit = {
    partitionColsForHive = inputData
  }


  private [this] def setNameOfStream(nameOfStream:String):Unit = {
    _nameOfStream = nameOfStream
  }

  private[this] def setStartingOffsets_(value: String): Unit = {
    _startingOffsets = value
  }

  private[this] def setCheckpointLocation_(value: String): Unit = {
    _checkpointLocation = value
  }

  private[this] def setOutputPath_(value: String): Unit = {
    _outputPath = value
  }

  private[this] def setHiveOutputPath_(value: String): Unit = {
    _hiveOutputPath = value
  }

  private[this] def setTopic_(value: String): Unit = {
    _topic = value
  }

  private[this] def setLogLevel_(value: String): Unit = {
    _logLevel = value
  }

  private[this] def setMaxOffSetsPerTrigger_(value: String): Unit = {
    _maxOffSetsPerTrigger = value
  }

  private[this] def setMinOffsetsPerTrigger_(value: String): Unit = {
    _minOffsetsPerTrigger = value
  }

  private[this] def setPollTime_(value: String): Unit = {
    _pollTime = value
  }

  private[this] def setTriggerDuration_(value: String): Unit = {
    _triggerDuration = value
  }


  private[this] def setHivetableName_(value: String): Unit = {
    _hivetableName = value
  }

  private[this] def setMaxTriggerDelay_(value: String): Unit = {
    _maxTriggerDelay = value
  }

  private[this] def setCleanStart(value:String):Unit = {
    cleanStart = value
  }



  /**
   * A setter to set the arguments passed via terminal.
   * @param args An array of arguments.
   * @param nameOfStream Name of the class.
   * @param partitionColumnsForHive Columns that are used for partitioning the hive table.
   */
  def setArguments(args:Array[String],nameOfStream:String,partitionColumnsForHive:Seq[String]):Unit = {
    if (args.length < 10)
      throw new InsufficientResourcesException(s"Not enough arguments. Minimum number of arguments required are  10 and were give ${args.length}. Arguments are arranged in following way: startingOffsets,checkPointLocation,outputPath,kafkaTopic.")
    if(nameOfStream == null)
      throw new NullPointerException("Name of the stream is null")
    setStartingOffsets_(args(0))
    setCheckpointLocation_(args(1))
    setOutputPath_(args(2))
    setHiveOutputPath_(args(3))
    setTopic_(args(4))
    setLogLevel_(args(5))
    setMaxOffSetsPerTrigger_(args(6))
    setMinOffsetsPerTrigger_(args(7))
    setPollTime_(args(8))
    setTriggerDuration_(args(9))
    setHivetableName_(args(10))
    setMaxTriggerDelay_(args(11))
    setCleanStart(args(12))
    confDirectory_(args(13))
    setNameOfStream(nameOfStream)
    setPartitionColsForHive(partitionColumnsForHive)
  }



  /** TODO
   * Creates a spark session from the given arguments.
   *
   * @param appName  Name of the spark application(required).
   * @param logger   An instance of logger to log the messages(required).
   * @param logLevel Defines the logLevel. By default INFO is used.
   * @return Returns a spark session.
   */
  protected [this] def getSparkSession(pushStreamInstance:PushStream): SparkSession = { //derby://127.0.0.1:9083  //thrift://el5177.bc:9083
    val session = SparkSession.builder()
                  .appName(pushStreamInstance.getNameOfStream)
                  .master(Configurator.getConfigurations("spark_stream_settings","spark.mode"))
                  .config("spark.sql.parquet.binaryAsString",true)
                  .config("spark.sql.parquet.mergeSchema",true)
                  .config("spark.sql.optimizer.metadataOnly",false)
                  .config("spark.sql.hive.convertMetastoreParquet.mergeSchema",true)
                   .config("spark.sql.caseSensitive",false)
                   .config("spark.sql.hive.convertMetastoreParquet",true)
                  .config("hive.metastore.uris", Configurator.getConfigurations("hive_settings","hive.metastore.uris"))
                  .config("spark.sql.streaming.fileSource.log.compactInterval",Configurator.getConfigurations("spark_stream_settings","spark.sql.streaming.fileSource.log.compactInterval"))
                  .config("spark.sql.streaming.metricsEnabled",Configurator.getConfigurations("spark_stream_settings","spark.sql.streaming.metricsEnabled"))
                  .config("spark.streaming.backpressure.enabled",Configurator.getConfigurations("spark_stream_settings","spark.streaming.backpressure.enabled"))
                  .config("spark.streaming.stopGracefullyOnShutdown",Configurator.getConfigurations("spark_stream_settings","spark.streaming.stopGracefullyOnShutdown"))
                  .enableHiveSupport().getOrCreate()
    session.conf.set("spark.sql.parquet.binaryAsString",true)
    session.conf.set("spark.sql.parquet.mergeSchema",true)
    session.conf.set("spark.sql.optimizer.metadataOnly",false)
    session.sparkContext.setLogLevel(pushStreamInstance.getLogLevel)
    session
  }




  /**
   *Registers a stream listener which will be executed on the end of each micro-batch.
   * @param sparkSession The sparksession which will be used to add streaming query listener.
   * @param pushStreamInstance The instance of a specific stream class.
   */
  def registerStreamListener(sparkSession: SparkSession, pushStreamInstance:PushStream): Unit = {
    if(ProcessDataForHive.shouldRemoveMetadataFile(pushStreamInstance)) {
       ProcessDataForHive.removeMetaDataFile(sparkSession, pushStreamInstance,pushStreamInstance.HDFS_BASE_PATH)
       Thread.sleep(2000)
    }
    if (!pushStreamInstance.shouldStartFromBeginning()  && ProcessDataForHive.isBatchJobLagging(sparkSession,pushStreamInstance)){
        println("Processing some lagging data. When stream query will start, you will be notified.")
        ProcessDataForHive.processLaggingData(sparkSession,pushStreamInstance)
       }
    sparkSession.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = println("Query with the id " + event.id + " has started.")
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val sourcePathBatchJob: String = event.progress.sink.description.replace("[", "").replace("]", "").replace("FileSink", "")
        ProcessDataForHive.processAndStoreData(sparkSession, event.progress.batchId, sourcePathBatchJob,pushStreamInstance)
      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = print("Query with the id " + event.id + " has stopped executing.")
    })
   }


  /**
   * Returns the raw dataframe which is created by ingesting data from kafka.
   * @return the dataframe created by ingesting data from kafka.
   */
  def getRawDFfromSession():DataFrame = createStreamContext(getSparkSession(this), this.getTopic, this.getStartingOffsets)

  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess


  /**
   * A function that initializes the UDFs.
   */
  def initUDFs():Unit = {
      addFields = udf(addValueToJsonFromSourceKeyToTargetKey(_: String, _: String, _: String): String)
      getDateFrom = udf(extractDate(_: String, _: String): String)
      getHourFrom = udf(extractHour(_: String, _: String): String)
      removeIllegalCharacters = udf(removeIllegalCharactersFromJson(_: String): String)
      addColumnValueToJson = udf(addKeyValueToJsonString(_: String, _: String, _: String): String)
      replaceValueInJson = udf(replaceValueInJsonWithNewValue(_: String, _: String, _: String): String)
      updateEpochField = udf(updateEpochTimeField(_:String):String)
      hasCol = udf(hasColumn(_:DataFrame,_:String):Boolean)
  }



  /**
   * A functions that selects only specific fields from the dataframe that will be writting to the disk.
   * @param silverDF The input dataframe.
   * @return the dataframe with only selected fields, determined by each class.
   */
  def refineDF(silverDF:DataFrame):DataFrame

  /**
   * Creates a stream context and returns the value field from the kafka source.
   * @param sparkSession SparkSession object.
   * @param topic        The kafka topic to which it will subscribe.
   * @param offset       Offset strategy, it can latest of earliest.
   * @return Returns a dataframe consisting of value column which is received from kafka.
   */
  def createStreamContext(sparkSession: SparkSession, topic: String, offset: String): DataFrame = {
    sparkSession.readStream
      .format(Configurator.getConfigurations("kafka_settings","kafka.source"))
      .option("kafka.bootstrap.servers", Configurator.getConfigurations("kafka_settings","kafka.bootstrap.servers"))
      .option("kafka.security.protocol",Configurator.getConfigurations("kafka_settings","kafka.security.protocol"))
      .option("kafka.sasl.mechanism",Configurator.getConfigurations("kafka_settings","kafka.sasl.mechanism"))
      .option("kafka.sasl.kerberos.service.name",Configurator.getConfigurations("kafka_settings","kafka.sasl.kerberos.service.name"))
      .option("kafka.ssl.protocol",Configurator.getConfigurations("kafka_settings","kafka.ssl.protocol"))
      .option("kafka.ssl.enabled.protocol",Configurator.getConfigurations("kafka_settings","kafka.ssl.enabled.protocol"))
      .option("subscribe", topic)
      .option("startingOffsets", offset)
      .option("minOffsetsPerTrigger",this.getMinOffsetsPerTrigger.toLong)
      .option("maxOffsetsPerTrigger", this.getMaxOffSetsPerTrigger.toLong)
      .option("maxTriggerDelay",this.getMaxTriggerDelay.toLong)
      .option("spark.streaming.kafka.consumer.poll.ms",this.getPollTime.toLong)
      .option("spark.streaming.backpressure.enabled",true)
      .option("startingOffsetsByTimestampStrategy","latest")
      .option("spark.streaming.receiver.writeAheadLog.enable",true)
      .option("spark.streaming.stopGracefullyOnShutdown",true)
      .option("spark.sql.streaming.fileSource.log.compactInterval",10000)
      .option("failOnDataLoss",false)
      .load()
      .select(col("value").cast("string"))
  }


  /**
   * Remove the following characters from the input string.(@ will be replaced by "" and "." will be replaced by "_")
   *
   * @param input Input JSON string.
   * @return Returns the modified JSON string.
   */
  def removeIllegalCharactersFromJson(input: String): String = {
      catching(classOf[Exception]).withTry(parse(input).values.asInstanceOf[Map[String, Any]]) match {
        case Success(output) => Json(DefaultFormats).write(removeCharactersFromJson(output))
        case Failure(exc) => throw new IllegalStateException("Failed to parse the provided JSON: " + exc.toString)
    }
  }


  /**
   * Takes a JSON string, an input key and input value. Replaces the value of the given key with the given value. Nothing is changed
   * if the key does not exist.
   * @param input         The input JSON string.
   * @param key           The key of which the value will be replaced.
   * @param replacerValue The value which will be written with the corresponding key.
   * @return Returns the replaced key. Incase an invalid JSON string is passed then null object will be returned.
   */
  def replaceValueInJsonWithNewValue(input: String, key: String, replacerValue: String): String = {
      catching(classOf[Exception]).withTry(parse(input).values.asInstanceOf[Map[String, Any]]) match {
        case Success(output) => Json(DefaultFormats).write(replaceKeyValueInMapWithNewValue(output, key, replacerValue))
        case Failure(exc) => throw new IllegalStateException("Failed to parse the provided JSON string. "+ exc.toString)
    }
  }


  /**
   * Takes a JSON string, a value and a key and adds a new entry to the data.
   *
   * @param input The input JSON string.
   * @param value The input value to be added to JSON string.
   * @param key   The input key for which the value will be added.
   * @return A JSON string containing the new key and value.
   */
  def addKeyValueToJsonString(input: String, value: String, key: String): String = {
      catching(classOf[Exception]).withTry(parse(input).values.asInstanceOf[Map[String, Any]]) match {
        case Success(output) => Json(DefaultFormats).write(output + (key -> value))
        case Failure(exc) => throw new IllegalStateException("Could not parse the json string in PushStream class. " +exc.toString)
    }
  }


  /**
   * Takes a JSON string and a key and returns the value corresponding to that key. If the key does not exist then null will be
   * returned.
   * @param inputJson An input JSON string.
   * @param key       An input key.
   * @return A value corresponding to the given key.
   */
  private [this] def getValueFromJsonByKey(inputJson: String, key: String): String = {
    catching(classOf[Exception]).withTry(parse(inputJson).values.asInstanceOf[Map[String, Any]]) match {
      case Success(output) => UDFUtil.getValueFromMap(output, key)
      case Failure(exc) => throw new IllegalStateException("Could not parse the json string in PushStream class. " +exc.toString)
    }
  }





  private [this] def addSourceKeyValueToTargetKey(input: Map[String, Any], sourceKey: String, targetKey: String): String = {
    def cleanMap(input:Map[String,Any],target:String): Map[String,Any] = {
      var cleanedMap = Map.empty[String,Any]
      for((key,value) <- input){
        value match {
          case map: Map[String, Any] => cleanedMap = cleanedMap + (key -> cleanMap(map, target))
          case _ => if (!key.equals(target) && !value.isInstanceOf[Map[String, Any]])
            cleanedMap = cleanedMap + (key -> value)
        }
      }
      cleanedMap
    }
    val cleanedMap = cleanMap(input,targetKey)
    Json(DefaultFormats).write(cleanedMap + (targetKey -> UDFUtil.getValueFromMap(cleanedMap, sourceKey)))
  }


  private [this] def replaceKeyValueInMapWithNewValue(input: Map[String, Any], newKey: String, newValue: String): Map[String, Any] = {
    input.map {
      case (key, value: Map[String@unchecked, _]) => key -> replaceKeyValueInMapWithNewValue(value, newKey, newValue)
      case (key, value) =>
        if (key == newKey)
          key -> newValue
        else
          key -> value
      }
    }

  private [this] def removeCharactersFromJson(input: Map[String, Any]): Map[String, Any] = {
    input.map {
      case (key, value: Map[String@unchecked, _]) => key.replaceAll("@", "").replaceAll("[\\s.]", "_") -> removeCharactersFromJson(value)
      case (key, value) => key.replaceAll("@", "").replaceAll("[\\s.]", "_").replaceAll("-","_")-> value
      }
    }


  /**
   * TO REFACTOR.
   */
  def removeDateOlderThanPreviousDate(currentDate:String):Boolean = {
    val hour = currentDate.split(" ")(0)
    val minutes = currentDate.split(" ")(1)
    meteDataMap.get(hour) match {
      case Some(hourSet:collection.mutable.Set[String]) => !( hourSet.contains(minutes))
      case None => true
    }
   }


  /**
   * Takes a JSON string, source key and a target key. It extracts the value associated with source key, from the JSON string and inserts the value with the target key in the JSON string.
   * @param input The input JSON string.
   * @param sourceKey Source key.
   * @param targetKey Target key.
   * @return
   */
  def addValueToJsonFromSourceKeyToTargetKey(input: String, sourceKey: String, targetKey: String): String = {
      UDFUtil.existField(input: String, targetKey: String) match {
        case true => input
        case false => catching(classOf[Exception]).withTry(addSourceKeyValueToTargetKey(parse(input).values.asInstanceOf[Map[String, String]], sourceKey, targetKey)) match {
          case Success(result) => result
          case Failure(_) => input
        }
      }
    }


  /**
   * Filters the rows of the dataframe on the basis of mac or mac_address where each row corresponds to a JSON string.
   * @param rawDF The input dataframe.
   * @return the dataframe with rows which have either mac or mac_address field and corresponding value.
   */
  def filterOnMac(rawDF:DataFrame):DataFrame = rawDF.filter(row => UDFUtil.existField(row.get(0).toString, "mac") ||
                                                                   UDFUtil.existField(row.get(0).toString, "mac_address")).toDF()


  /***
   * Filters the rows of the dataframe on the basis of time_field that is passed to the function.
   * @param df The input dataframe.
   * @param timeField the time field which will be checked.
   * @return the dataframe filtered on the given time field.
   */
  def filterOnTime(df:DataFrame,timeField:String): DataFrame =
    df.filter(row => {UDFUtil.existField(row.get(0).toString, timeField) &&
      UDFUtil.isValidDate(epochTimeToDate(row.get(0).toString, timeField))
    }).toDF()


  /**
   * Takes a dataframe and a user defined function that removes the illegal characters from JSON and removes the illegal characters from JSON.
   * @param goldDF the input dataframe.
   * @param removeIllegalCharacters A user defined function that will be used to remvoe illgal characters.
   * @return the dataframe without any illegal character.
   */
  def removeIllegalChars(goldDF:DataFrame, removeIllegalCharacters:UserDefinedFunction):DataFrame = {
      goldDF.withColumn("value", removeIllegalCharacters(col("value")))
    }


  /**
   * Adds date and hour to a dataframe.
   * @param df the dataframe to which the value will be added.
   * @param addColumnValueToJson user defined function that adds fields to json string.
   * @param dateColName name of the field that indicates the date.
   * @return the dataframe consisting of JSON string with date and hour added to JSON.
   */
  def addDfColumnValueToJSONString(df:DataFrame, addColumnValueToJson:UserDefinedFunction, dateColName:String):DataFrame = {
    df.withColumn("value", addColumnValueToJson(col("value"), col("date"), lit(dateColName)))
      .withColumn("value", addColumnValueToJson(col("value"), col("hour"), lit("hour")))
    }



  /**
   * Adds date and hour to JSON from epoch_field.
   * @param df the input dataframe consisting of JSON string that contains epoch_field.
   * @param field the epoch_field.
   * @param getDateFrom A user defined function that is used to get date from JSON.
   * @param getHourFrom A user defined function that is used to get hour from JSON.
   * @return the dataframe consisting of JSON string with date and hour field added.
   */
  def addDateAndHourToJsonFromEpochField(df:DataFrame, field:String, getDateFrom:UserDefinedFunction, getHourFrom:UserDefinedFunction):DataFrame = {
      df.withColumn("date", getDateFrom(col("value").cast("string"), lit(field)))
        .withColumn("hour", getHourFrom(col("value").cast("string"), lit(field)))
     }

  /**
   * It extracts the epoch_time field from the JSON string and convert it to time and returns as a string.
   * @param inputJson the input JSON string.
   * @param key the key.
   * @return the string containing the date.
   */
  def epochTimeToDate(inputJson: String, key: String): String = {
    val valueByJsonKey = getValueFromJsonByKey(inputJson, key)
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    if (valueByJsonKey.length < 13) {
      dateFormat.format(valueByJsonKey.toLong * 1000)
    } else {
      dateFormat.format(valueByJsonKey.toLong)
    }
  }

  def updateEpochTimeField(epochTime:String):String = {
    if (epochTime.length < 13) {
      (epochTime.toLong * 1000).toString
    } else {
      epochTime.toLong.toString
    }
  }

  /**
   * Finds the date from the JSON string and returns it as a string.
   * @param inputJson An input JSON string.
   * @param key A string that presents the time field.
   * @return Returns ONLY date extracted from the JSON string.
   */
  def extractDate(inputJson: String, key: String): String = epochTimeToDate(inputJson, key).split(" ")(0)


  /**
   * Finds the hour from the JSON string and returns it as a string.
   * @param inputJson An input JSON string.
   * @param key A string that presents the time field.
   * @return Returns ONLY hour extracted from the JSON string.
   */
  def extractHour(inputJson: String, key: String): String = epochTimeToDate(inputJson, key).split(" ")(1).split(":")(0)


  /**
   * Writes the dataframe as parquet to disk.
   * @param goldDF the dataframe that needs to be written to disk.
   */
  def writeToHDFS(goldDF: DataFrame): Unit = {
    goldDF.repartition(1).writeStream
      .outputMode("append")
      .partitionBy("date", "hour")
      .format("text")
      .option("compression", "none")
      .option("path", this.getStreamOutputPath)
      .option("checkpointLocation", this.getCheckpointLocation)
      .trigger(Trigger.ProcessingTime(this.getTriggerDuration + " seconds"))
      .start()
      .awaitTermination()
  }


}
