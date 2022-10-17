package streams.commonStreams

import org.apache.spark.sql.SparkSession

object UpdatePartitioner {


  def getSession():SparkSession = {

    val session = SparkSession.builder()
      .appName("HivePartitionManagerPushFlows")
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
    session.sparkContext.setLogLevel("WARN")
    session
  }

  def main(args:Array[String]):Unit = {
    val tablesToRepair:String = args(0)
    val tables:Array[String] = tablesToRepair.split(",")
    val session = getSession()
    val sqlContext = session.sqlContext
    tables.foreach(e => {
      sqlContext.sql("MSCK REPAIR TABLE "+e)
      println("Repaired table " + e)
    })
  }

}
