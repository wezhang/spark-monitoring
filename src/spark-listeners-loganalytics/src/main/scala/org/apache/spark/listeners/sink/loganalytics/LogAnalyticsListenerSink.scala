package org.apache.spark.listeners.sink.loganalytics

import com.microsoft.pnp.client.loganalytics.{LogAnalyticsClient, LogAnalyticsSendBufferClient}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.listeners.sink.SparkListenerSink
import org.json4s.{JsonAST, DefaultFormats}
import org.json4s.jackson.JsonMethods.compact

import scala.util.control.NonFatal

class LogAnalyticsListenerSink(conf: SparkConf) extends SparkListenerSink with Logging {
  private val config = new LogAnalyticsListenerSinkConfiguration(conf)
  implicit val formats = DefaultFormats


  protected lazy val logAnalyticsBufferedClient = new LogAnalyticsSendBufferClient(
    new LogAnalyticsClient(
      config.workspaceId, config.secret),
    config.logType
  )

  override def logEvent(event: Option[JsonAST.JValue]): Unit = {
    try {
      event match {
        case Some(j) => {
          val event = (j \ "Event").extract[String]
          if(event=="SparkListenerJobStart" || event=="SparkListenerJobEnd")
          {
            val jsonString = compact(j)
            logDebug(s"Sending event to Log Analytics: ${jsonString}")
            logAnalyticsBufferedClient.sendMessage(jsonString, "SparkEventTime")
          }
        }
        case None =>
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Error sending to Log Analytics: $e")
    }
  }
}
