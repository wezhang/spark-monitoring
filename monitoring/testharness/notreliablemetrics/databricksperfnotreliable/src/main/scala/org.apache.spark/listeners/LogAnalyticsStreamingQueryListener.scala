package org.apache.spark.listeners

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.{LogAnalytics, SparkConf,LogAnalyticsPerf,LogAnalyticsPerf2}
import org.apache.spark.LogAnalyticsListenerConfiguration

class LogAnalyticsStreamingQueryListener(sparkConf: SparkConf) extends StreamingQueryListener
  with Logging with LogAnalytics {




 val config = new LogAnalyticsListenerConfiguration(sparkConf)

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
 //   val logger = new LogAnalytics()
    logEvent(event)
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
  //  val logger = new LogAnalytics()
    logEvent(event)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
   // val logger = new LogAnalytics()
    logEvent(event)
  }
}
