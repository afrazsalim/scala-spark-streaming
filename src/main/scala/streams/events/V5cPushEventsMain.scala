package streams.events

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit, udf}


object V5cPushEventsMain extends Events {
  var allowedEvents: Array[String] = _


  def main(args: Array[String]): Unit = {
    super.setArguments(args,"kafka_to_hive_v5c_events",Seq("event_date","event_type"))
    super.initUDFs()
    allowedEvents = getAllowedEventsFromFile(HDFS_BASE_PATH + args(13), this)
    val goldDF = super.applyCommonEventsOperations(this)
                      .withColumn("value", replaceValueInJson(col("value"), lit("hw_type"), lit("v5c")))
    super.writeToHDFS(goldDF)
  }


  override def refineDF(silverDF: DataFrame): DataFrame = {
    val currentAllowedEvents:Array[String]="AD-REPLACEMENT,AD-TRIGGER,APP-LAUNCH,BOM,BOOTFLOW-ERROR,BROKER-UPDATE,BTA-CALL,CATCH-UP,CEC,CONNECTED-DEFLECT,CONSUMER-PVR,CPVR-SYS,CPVR-USER,CPVR-USR-ERROR,CPVR-VIEW-ERROR,CUTV,DR-SELF,DRM-ERROR,DTV-CH,DTV-GUIDE,DTV-SEARCH,HARD-PWR-OFF,HDMI,HOTKEY,HUB,INFO-URL,INTERACTIVITY-PAGE,ISOLATED-STATE,LANG-SET-SP,LANGUAGE-SETTINGS,LINEAR,MCAST-ERR,MENU,MULTICAST-ERROR,MUSIC-GUIDE,NETFLIX,NETWORK-PVR,NETWORK_PVR,NO-SIGNAL,NPVR-SYS,NPVR-USER,NPVR-USR-ERROR,NPVR-VIEW-ERROR,NTFLX,ON-DEMAND,OPTIN,P,PLAYER,POWER-CYCLE,PRGM-CH,PWR-ACT-OFF,PWR-ACT-STBY,PWR-NETSTBY-ACT,PWR-NETSTBY-OFF,PWR-NETSTBY-STBY-WOL,PWR-NETSTBY-STBY-WUT,PWR-OFF-ACT,PWR-ON,PWR-SLEEP-ACT,PWR-SLEEP-OFF,PWR-SLEEP-STBY-WUT,PWR-STBY-ACT,PWR-STBY-NETSTBY-ERR,PWR-STBY-OFF,PWR-STBY-SLEEP-ERR,QOE,RADIO,RATE-REQUEST,RECO-BROWSE,RECOMMENDATION-BROWSE,RTSP,SEARCH-BROWSE,SETTINGS,SLEEP,SOFTWARE-UPGRADE,STANDBY,STBSTATUS-BASICDTV,STBSTATUS-BasicDTV,STBSTATUS-FULLSERVICE,SW-UPGRADE,SYSTEM-ERROR,TA-REPL,TA-TRIG,TIME-SHIFT,TSTV,TSU-FINISHED,TSU-START,UPNP,UPSELL,VOD-GUIDE,VOD-PREVIEW,VOD-RENT-VHP,VOD-RENTAL,VOD-VIEW,VQE-CONFIG-ERROR".split(",")
    super.refineDF(silverDF)
      .drop("HTTP_response_code")
      .drop("null")
      .drop("(null)")
      .drop("events.*")
      .withColumn("mac", col("mac_address"))
      .withColumn("event_type", col("sub_type"))
      .withColumn("event_epoch_time",col("event_time"))
      .filter(row => {
      val currentEvents:Array[String] = row.toString().replace("]","").split(",")
      isValidEvent(currentEvents,currentAllowedEvents)
    })
  }
}
