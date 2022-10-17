package streams.commonStreams
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{abs, col}
import org.apache.spark.sql.types.{DecimalType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.json4s.jackson.JsonMethods.parse

import java.net.URI
import java.text.SimpleDateFormat
import scala.annotation.tailrec
import scala.util.control.Exception.catching
import scala.util.{Failure, Success}

/**
 * An object that is used to process data for hive.
 */
object ProcessDataForHive {


  private [this] var PREVIOUS_DATE = "2011-09-27 18:00:00"
  /**
   * Keeps metadata about last 50 processed files in memory.
   */
  var meteDataMap: collection.mutable.Map[String, collection.mutable.Set[String]] = collection.mutable.Map.empty[String, collection.mutable.Set[String]]
  private [this] val simpleDataFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private [this] val onlyDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  private [this] val MAX_MAP_SIZE = 60
  private [this] var REMOVED_METADATA_FILE = false
  private[this] var counter = 0
  private[this] var numberOfSkips = 0


  private [this] val schema: StructType = StructType(Array(
    StructField("date", StringType, nullable = false),
    StructField("hour", StringType, nullable = false),
    StructField("value", StringType, nullable = false)
  ))


  implicit class DataFrameOperations(df: DataFrame) {
    def dropDuplicateCols(rmvDF: DataFrame): DataFrame = {
      val cols = df.columns.groupBy(identity).mapValues(_.length).filter(_._2 > 1).keySet.toSeq
      @tailrec
      def deleteCol(df: DataFrame, cols: Seq[String]): DataFrame = {
        if (cols.isEmpty) df else deleteCol(df.drop(rmvDF(cols.head)), cols.tail)
      }
      deleteCol(df, cols)
    }
  }



  /**
   * Add partitions to the hive table.
   * @param sparkSession the spark session object.
   * @param silverDF input dataframe.
   * @param pushStreamInstance instance of the streaming app.
   */
  private [this] def alterPartitionsInHiveTable(sparkSession: SparkSession, silverDF: DataFrame, pushStreamInstance: PushStream): Unit = {
    val sqlContext = sparkSession.sqlContext
    if(counter%80 == 0) {
      sqlContext.sql("MSCK REPAIR TABLE "+pushStreamInstance.getHivetableName)
      println("Changed partitions")
    }
    counter = counter +1
    if (meteDataMap.size >= MAX_MAP_SIZE-1)
        removeOldDates()
  }


  private [this] def getListOfFileFromBatchFile(sparkSession: SparkSession, fileName: String): Array[String] = sparkSession.sparkContext.textFile(fileName).filter(f => f.length > 3).collect()


  private [this] def getModificationTime(sparkSession: SparkSession, fileName: String, hdfsURI: String): Long = {
    val fs = FileSystem.get(new URI(hdfsURI), sparkSession.sparkContext.hadoopConfiguration)
    catching(classOf[Exception]).withTry(fs.listStatus(new Path(fileName)).map(x => x.getModificationTime).toList) match {
      case Success(x) => x.head
      case Failure(exc) => throw new IllegalStateException("Could not get file modification time. " + exc.toString)
    }
  }

  private[this] def getMetaDataFromRDD(rddFromFile: (Array[String], Long)): Array[(String, String, String)] = {
    rddFromFile._1.map(e => {
      val convertToMap = parse(e).values.asInstanceOf[Map[String, Any]]
      val path = convertToMap.get("path") match {
        case Some(path) => path
        case None => throw new IllegalArgumentException("Could not find path from the map.")
      }
      val timeMode = convertToMap.get("modificationTime") match {
        case Some(time) => time
        case None => throw new IllegalArgumentException("Could not find modificationTime from the map.")
      }
      (path.toString, timeMode.toString, rddFromFile._2.toString)
    })
  }


  private [this] def getListOfFileFromCurrentBatch(sparkSession: SparkSession, validBatchID: Long, sourcePathBatchJob: String, hdfsURI: String): Array[(String, String, String)] = {
    val file = sourcePathBatchJob + "/_spark_metadata/" + validBatchID.toString
    val rddFromFile: (Array[String], Long) = catching(classOf[Exception]).withTry(getListOfFileFromBatchFile(sparkSession, file)) match {
      case Success(output) => (output, getModificationTime(sparkSession, file, hdfsURI))
      case Failure(_) => catching(classOf[Exception]).withTry(getListOfFileFromBatchFile(sparkSession, file + ".compact")) match {
        case Success(out) => (out, getModificationTime(sparkSession, file + ".compact", hdfsURI))
        case Failure(exc) => throw new IllegalStateException("Could not read the file " + file + ". " + exc.toString)
      }
    }
    getMetaDataFromRDD(rddFromFile)
  }

  private [this] def processFile(silverDF: DataFrame, pushStreamInstance: PushStream): Unit = {
    val dataFrame: DataFrame = pushStreamInstance.refineDF(silverDF)
    val finalDF = dataFrame.select(dataFrame.columns.map(c => col(c).cast(StringType)) : _*)
                           .select(dataFrame.columns.map(x => col(x).as(x.toLowerCase)): _*)
    finalDF.show(2)
    finalDF.write.partitionBy(pushStreamInstance.getPartitionColsForHive.map(name => name): _*).option("compression","none").mode("append").parquet(pushStreamInstance.getHiveOutputPath)
  }


  private [this] def processAllFiles(sparkSession: SparkSession, listOfFilesToProcess: Array[String], pushStreamInstance: PushStream): Unit = {
    import sparkSession.implicits._
    val rawDF = sparkSession.read.schema(schema).format("text").load(listOfFilesToProcess: _*).selectExpr("CAST(value as STRING)").as[String]
    val jsonDF = sparkSession.read.option("inferSchema", false).option("mergeSchema", true).json(rawDF)
    catching(classOf[Exception]).withTry(processFile(jsonDF, pushStreamInstance)) match {
      case Success(_) =>   alterPartitionsInHiveTable(sparkSession, jsonDF, pushStreamInstance)
      case Failure(exc) =>
        if (numberOfSkips > 5)
             throw new IllegalStateException("Could not write files for hive table. " + exc)
        else numberOfSkips = numberOfSkips + 1
    }
  }



  private [this] def removeOldDates(): Unit = {
    while (meteDataMap.size > MAX_MAP_SIZE / 2) {
      val dateKeys: List[String] = meteDataMap.keys.toList
      val oldestDate = getOldestTimeStamp(dateKeys)
      meteDataMap.remove(oldestDate)
    }
  }



  private[this] def getOldestTimeStamp(timestamps: List[String]): String = {
    var first = "None"
    if (timestamps.isEmpty)
      return PREVIOUS_DATE
    else if (timestamps.length < 2 && timestamps.nonEmpty)
      return timestamps.head
    else {
      first = timestamps.head
      for (i <- 1 until timestamps.length)
        if (onlyDateFormat.parse(first).compareTo(onlyDateFormat.parse(timestamps(i))) > 0)
          first = timestamps(i)
    }
    first
  }




  /**
   * Saves the batch metadata to the disk.
   * @param sparkSession the spark session object.
   * @param batchID current batch id.
   * @param pushStreamInstance instance of the streaming application.
   * @param batchModTime process time of the batch.
   */
  private [this] def saveBatchInformationOnHDFS(sparkSession: SparkSession, batchID: Long, pushStreamInstance: PushStream, batchModTime: String): Unit = {
    import sparkSession.implicits._
    val schema = StructType(Array(StructField("last_batch_id", StringType, nullable = true), StructField("last_mod_time", StringType, nullable = true)))
    val jsonRecord = s"""{"last_batch_id":${batchID.toString},"last_mod_time":$batchModTime}"""
    val metaDataDF = sparkSession.read.schema(schema).json(Seq(jsonRecord).toDS)
    metaDataDF.write.mode(SaveMode.Overwrite).json(pushStreamInstance.getHiveOutputPath + "/_meta_data_"+pushStreamInstance.getNameOfStream+"/")
  }


  /**
   * Fetches the metadata files from current batchID and process the files belonging to that metadata.
   * @param sparkSession the sparkSession instance.
   * @param validBatchID current batchId.
   * @param sourcePathBatchJob path from where data will be read.
   * @param lastModicationTime last modification time of a file.
   * @param pusStreamInstnace the instance of a stream.
   */
  private [this] def fetchFilesFromCurrentBatchIDAndProcessFiles(sparkSession: SparkSession, validBatchID: Long, sourcePathBatchJob: String, lastModicationTime: String, pusStreamInstnace: PushStream): Unit = {
    val listOfFilesToProcess = getListOfFileFromCurrentBatch(sparkSession, validBatchID, sourcePathBatchJob, pusStreamInstnace.HDFS_BASE_PATH)
    val previouModTime = simpleDataFormat.parse(simpleDataFormat.format(lastModicationTime.toLong))
    val fs = FileSystem.get(new URI(pusStreamInstnace.HDFS_BASE_PATH), sparkSession.sparkContext.hadoopConfiguration)
    val filterFilesPerTimeStamp = listOfFilesToProcess.filter(elem => {
    val latestModTime = simpleDataFormat.parse(simpleDataFormat.format(elem._2.toLong))
      latestModTime.compareTo(previouModTime) > 0}).filter(e=> {
      fs.exists(new Path(e._1))
    })
    if (filterFilesPerTimeStamp.length > 0)
      filterFilesPerTimeStamp.grouped(1000).toList.foreach(path => {
        catching(classOf[Exception]).withTry(processAllFiles(sparkSession, path.map(e=>e._1), pusStreamInstnace)) match {
          case Success(_) => saveBatchInformationOnHDFS(sparkSession, validBatchID, pusStreamInstnace, path.map(e=>e._3).toList.head)
                             println("Processed " + 1000+ " files.")
          case Failure(exc) =>
            println(exc)
            sparkSession.streams.active.foreach(e => e.stop())
            sparkSession.sparkContext.stop()
            System.exit(-1)
            throw new IllegalStateException("Could not process all files. NUmber of files to process were " +filterFilesPerTimeStamp.length+". " + exc)
        }
      })
   }


  /**
   * Removes the metadata file.
   * @param sparkSession the spark session object.
   * @param pushStreamInstance instance of the streaming job.
   * @param hdfsURI HDFS base path.
   */
  def removeMetaDataFile(sparkSession: SparkSession, pushStreamInstance:PushStream,hdfsURI:String) : Unit = {
    println("Removing all previous metadata")
   val fs = FileSystem.get(new URI(hdfsURI), sparkSession.sparkContext.hadoopConfiguration)
   if(fs.exists(new Path(pushStreamInstance.getHiveOutputPath + "/_meta_data_"+pushStreamInstance.getNameOfStream+"/") )&& fs.isDirectory(new Path(pushStreamInstance.getHiveOutputPath + "/_meta_data_"+pushStreamInstance.getNameOfStream+"/")))
      fs.delete(new Path(pushStreamInstance.getHiveOutputPath + "/_meta_data_"+pushStreamInstance.getNameOfStream+"/"),true)
   if(fs.exists(new Path(pushStreamInstance.getCheckpointLocation) )&& fs.isDirectory(new Path(pushStreamInstance.getCheckpointLocation)))
      fs.delete(new Path(pushStreamInstance.getCheckpointLocation),true)
   if(fs.exists(new Path(pushStreamInstance.getStreamOutputPath+"/_spark_metadata") )&& fs.isDirectory(new Path(pushStreamInstance.getStreamOutputPath+"/_spark_metadata")))
     fs.delete(new Path(pushStreamInstance.getStreamOutputPath+"/_spark_metadata"),true)
     REMOVED_METADATA_FILE = true
     println("Removed old metadata file")
  }



  /**
   * An inspector to check if the metadata file should be removed.
   * @param streamInstance the stream instance.
   * @return true or false.
   */
   def shouldRemoveMetadataFile(streamInstance: PushStream):Boolean = streamInstance.shouldStartFromBeginning() && !REMOVED_METADATA_FILE


  /**
   * A getter to get the metadata of previous batch.
   * @param sparkSession the sparkSession object.
   * @param pushStreamInstance instance of push stream.
   * @return a map containing the last_batch_id and last_mod_time fields with corresponding values.
   */
  private [this] def getPreviousBatchMetaData(sparkSession: SparkSession, pushStreamInstance: PushStream): Map[String, String] = {
    val listoOfMetaData = sparkSession.sparkContext.textFile(pushStreamInstance.getHiveOutputPath + "/_meta_data_"+pushStreamInstance.getNameOfStream+"/part*.json").collect().toList
    assert(listoOfMetaData.length == 1)
    parse(listoOfMetaData.head).values.asInstanceOf[Map[String, String]]
  }


  protected def existsFiles(sparkSession:SparkSession,sourcePath:String,batchID:Long,pushStreamInstance: PushStream):Boolean = {
    val file = sourcePath + "/_spark_metadata/" + batchID.toString
    val fs = FileSystem.get(new URI(pushStreamInstance.HDFS_BASE_PATH), sparkSession.sparkContext.hadoopConfiguration)
    if(fs.exists(new Path(file) ))
       return true
    false
  }

  protected def getLatestIndexFromStream(sparkSession:SparkSession,sourcePath:String,pushStreamInstance:PushStream):Long = {
    val path = sourcePath + "/_spark_metadata/"
    val fs = FileSystem.get(new URI(pushStreamInstance.HDFS_BASE_PATH), sparkSession.sparkContext.hadoopConfiguration)
    catching(classOf[Exception]).withTry(fs.listStatus(new Path(path)).filter(_.isFile).map(e => e.getPath.toString.split("/").last).filter(_.contains(".compact")).map(e => e.split(".compact").head.toLong).sorted) match {
      case Success(output) => output.sorted.last
      case Failure(e)=> throw new IllegalStateException("Could not get latest index. " + e)
    }
  }

  def isBatchJobLagging(sparkSession: SparkSession, pushStreamInstance: PushStream): Boolean = {
    val batchID = catching(classOf[Exception]).withTry(getLatestIndexFromStream(sparkSession, pushStreamInstance.getStreamOutputPath, pushStreamInstance)) match{
      case Success(output) => output
      case Failure(_) => -1
    }
    val validBatchId: Long = math.max(0, batchID - 1)
    catching(classOf[Exception]).withTry(getPreviousBatchMetaData(sparkSession, pushStreamInstance)) match {
      case Success(output: Map[String, String]) =>
        output.get("last_batch_id") match {
          case Some(previousBatchID) =>
            if (Math.abs(previousBatchID.toLong - validBatchId) > 5)
              return true
          case None => throw new IllegalStateException("Could not get the batch id from previous run.")
        }
      case Failure(_) => return false
    }
    false
  }

  def processLaggingData(sparkSession: SparkSession, pushStreamInstance: PushStream):Unit = {
      val batchID = getLatestIndexFromStream(sparkSession, pushStreamInstance.getStreamOutputPath, pushStreamInstance)
      catching(classOf[Exception]).withTry(getPreviousBatchMetaData(sparkSession, pushStreamInstance)) match {
        case Success(output: Map[String, String]) =>
          output.get("last_batch_id") match {
            case Some(_) => fetchFilesFromCurrentBatchIDAndProcessFiles(sparkSession, batchID, pushStreamInstance.getStreamOutputPath, output("last_mod_time"), pushStreamInstance)
          }
      }
    }


  def stopSparkStreamAndExitTheProgram(sparkSession:SparkSession):Unit = {
    println("Shutting down application to avoid data loss. Restart the application with same configuration in order work.")
    sparkSession.sparkContext.stop()
    System.exit(-1)
  }

  /**
   * Fetch and process the data for hive, which is stored by stream instance.
   *
   * @param sparkSession the spark session object.
   * @param batchId the micro-batch which was written by streaming instance.
   * @param sourcePath path from where the data needs to be pulled.
   * @param pushStreamInstance instance of the stream that wrote the micro-batch.
   * @throws IllegalStateException if previous batch ran succesfully and the batchId cannot be read from previous run in current run.
   * @throws IllegalArgumentException if the file itself containg the batchID cannot be read.
   */
  def processAndStoreData(sparkSession: SparkSession, batchId: Long, sourcePath: String, pushStreamInstance: PushStream): Unit = {
    val validBatchId: Long = math.max(0, batchId - 1)
    catching(classOf[Exception]).withTry(getPreviousBatchMetaData(sparkSession, pushStreamInstance)) match {
      case Success(output: Map[String, String]) =>
        output.get("last_batch_id") match {
          case Some(previousBatchID) =>
            if (previousBatchID.toLong < validBatchId)
               for(index <- previousBatchID.toLong+1 to validBatchId) {
                   println("Processing batch ID " +index + " of " +  validBatchId.toString)
                   fetchFilesFromCurrentBatchIDAndProcessFiles(sparkSession, index, sourcePath, output("last_mod_time"), pushStreamInstance)
                 }
            if(Math.abs(previousBatchID.toLong -validBatchId) > 10)
               stopSparkStreamAndExitTheProgram(sparkSession)
          case None => throw new IllegalStateException("Could not get the batch id from previous run.")
        }
      case Failure(_: org.apache.hadoop.mapred.InvalidInputException) =>
           println(" Failed branch : Previous batch ID is "+ 0+ " Current batch ID " +  validBatchId.toString)
          fetchFilesFromCurrentBatchIDAndProcessFiles(sparkSession, validBatchId, sourcePath, "163490925144", pushStreamInstance)
      case Failure(exc) => throw new IllegalArgumentException(exc.toString)
    }
  }
}
