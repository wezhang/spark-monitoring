package org.apache.spark.listeners

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.{LogAnalytics, SparkConf,LogAnalyticsPerf,LogAnalyticsPerf2}
//import org.apache.spark.LogAnalyticsListenerConfiguration
import org.apache.spark.LogAnalyticsListenerConfiguration2

class LogAnalyticsStreamingQueryListener(sparkConf: SparkConf) extends StreamingQueryListener
  with Logging /*with LogAnalytics*/ {
// @transient lazy val logger = new LogAnalyticsPerf()



  val config = new LogAnalyticsListenerConfiguration2(sparkConf)
  val logger = new LogAnalyticsPerf2(config)
// val logger = new LogAnalyticsPerf()

 // val config = new LogAnalyticsListenerConfiguration(sparkConf)

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
 //   val logger = new LogAnalytics()
    logger.logEvent(event)
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
  //  val logger = new LogAnalytics()
    logger.logEvent(event)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
   // val logger = new LogAnalytics()
    logger.logEvent(event)
  }
}
