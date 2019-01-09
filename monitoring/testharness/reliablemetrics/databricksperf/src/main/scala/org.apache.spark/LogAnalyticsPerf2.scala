package org.apache.spark

import java.text.SimpleDateFormat
import java.util._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.microsoft.pnp.loganalytics.{LogAnalyticsBufferedClient, LogAnalyticsClient}
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.streaming.scheduler.StreamingListenerEvent
import org.apache.spark.util.JsonProtocol
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse, _}

import scala.util.control.NonFatal


class LogAnalyticsPerf2(config: LogAnalyticsListenerConfiguration2) {

//  protected lazy val logAnalyticsClient = new LogAnalyticsClient(
//    "422d63b4-4d37-4c2f-8bbb-bcf06b8e5b8f", "g2qLqxzH0fGlAYZ9zfHlZNnesj1fOwACF7gwbHOdG+Z7YBuVa41a5XY82Oqr7lqv60WyrUPdXOQV2qPINH2B5A==")

  protected lazy val logAnalyticsClient = new LogAnalyticsClient(
    config.workspaceId, config.secret)

  protected lazy val client = new LogAnalyticsBufferedClient(
    logAnalyticsClient, "SparkListenerEvent",
    1024 * 1024 * 2, 10000)

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)


  def logEvent(event: AnyRef): Unit = {
    val s = try {
      val json = event match {
        case e: SparkListenerEvent => Some(JsonProtocol.sparkEventToJson(e))
        case e: StreamingListenerEvent => Some(parse(mapper.writeValueAsString(e)))
        case e => None
      }


      if (json.isDefined) {
        Some(compact(json.get))
      } else {
        None
      }
    } catch {
      case NonFatal(e) => None
    }

    if (s.isDefined) {
      val tz = TimeZone.getTimeZone("UTC")
      var df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.ms'Z'") // Quoted "Z" to indicate UTC, no timezone offset
      df.setTimeZone(tz)
      var nowAsISO = df.format(new Date())


      val dataJson = parse(s.get)

      val dataJson2 = dataJson merge render("DateValue" -> nowAsISO)
      // this is for testing purposes
      val dataJson3 = dataJson2 merge render("DateTimeProduced" -> nowAsISO)


  //    val str = compact(dataJson3)

 //     logAnalyticsClient.send(str, "sparktest1", "DateValue")

      client.sendMessage(compact(dataJson3), "DateValue")

    }

  }

}
