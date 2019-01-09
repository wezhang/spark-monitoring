package org.apache.spark





import org.apache.spark.internal.Logging



//---------------------------
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.util.JsonProtocol
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._



import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import com.google.gson.{JsonObject, JsonParser}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._






import com.microsoft.pnp.loganalytics.LogAnalyticsClient
import com.microsoft.pnp.loganalytics.LogAnalyticsBufferedClient
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.streaming.scheduler.StreamingListenerEvent
import org.json4s.jackson.JsonMethods.{compact, parse}
import java.util._
import java.time.{ZoneId, ZonedDateTime}
import org.json4s._
//import org.json4s.JsonDSL._
//import org.json4s.jackson.JsonMethods._
import scala.util.control.NonFatal
import java.util.Calendar
import java.text.SimpleDateFormat

trait LogAnalytics {
  this: Logging =>
  protected val config: LogAnalyticsConfiguration



  protected lazy val logAnalyticsClient = new LogAnalyticsClient(
    config.workspaceId, config.secret)

  protected lazy val client = new LogAnalyticsBufferedClient(
    logAnalyticsClient, "SparkListenerEvent",
    1024 * 1024 * 2, 10000)



  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "Event")
  private class StreamingListenerEventMixIn {}

  // Add the Event field since the StreamingListenerEvents don't extend the SparkListenerEvent trait
  mapper.addMixIn(classOf[StreamingListenerEvent], classOf[StreamingListenerEventMixIn])

  def logEvent(event: AnyRef) {
    val s = try {
      val json = event match {
        case e: SparkListenerEvent => Some(JsonProtocol.sparkEventToJson(e))
        case e: StreamingListenerEvent => Some(parse(mapper.writeValueAsString(e)))
        case e =>
   //       logWarning(s"Class '${e.getClass.getCanonicalName}' is not a supported event type")
          None

      }


      if (json.isDefined) {
        Some(compact(json.get))
      } else {
        None
      }
    } catch {
      case NonFatal(e) =>
        //logWarning(s"Error sending to Log Analytics: $e")
       // logWarning(s"event: $event")
        None
    }

    if (s.isDefined) {
      val tz = TimeZone.getTimeZone("UTC")
      var df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.ms'Z'") // Quoted "Z" to indicate UTC, no timezone offset
      df.setTimeZone(tz)
      var nowAsISO = df.format(new Date())



    val dataJson = parse(s.get)

    val dataJson2 = dataJson merge render ("DateValue" -> nowAsISO )
    val dataJson3 = dataJson2 merge render ("DateTimeProduced" -> nowAsISO)
//      val str = compact(dataJson3)

   //   logAnalyticsClient.send(str, "sparktest1", "DateValue")



      client.sendMessage( compact(dataJson3), "DateValue")

    }
  }







}
