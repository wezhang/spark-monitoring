package org.apache.spark

import com.microsoft.pnp.loganalytics.{LogAnalyticsBufferedClient, LogAnalyticsClient}
import org.json4s.jackson.JsonMethods.{compact, parse}
import java.util._
import java.time.{ZoneId, ZonedDateTime}

import org.json4s._

import scala.util.control.NonFatal
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.streaming.scheduler.StreamingListenerEvent
import org.apache.spark.util.JsonProtocol
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.internal.Logging
import java.text.SimpleDateFormat

import org.json4s.JsonDSL._
import org.json4s.jackson._
import org.json4s.jackson.renderJValue
import org.json4s.jackson.JsonMethods._


class LogAnalyticsPerf {

//  protected lazy val logAnalyticsClient = new LogAnalyticsClient(
//    "422d63b4-4d37-4c2f-8bbb-bcf06b8e5b8f", "g2qLqxzH0fGlAYZ9zfHlZNnesj1fOwACF7gwbHOdG+Z7YBuVa41a5XY82Oqr7lqv60WyrUPdXOQV2qPINH2B5A==")

  protected lazy val logAnalyticsClient = new LogAnalyticsClient(
    "6cdabfff-ca67-46d4-a18c-8e5b2d81b6bf", "b79df9t3b3jBZfeXPIi7Jp3BuejGPVGajFQdKIaEDMPzdCn22INMGhFcjJPqhpU9tKa9CxpcXuyxtmjcaTNQZw==")

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
      val dataJson3 = dataJson2 merge render("DateTimeProduced" -> nowAsISO)

      /*
  val data="{\"message\":\"goodmessage\",\"partitionId\":1,\"deviceid\":" +
    "\"simulated\"," +
    "\"avgtemperature\":74.78," +
    "\"DateTimeProduced\":\"" +
    nowAsISO +
    "\"," +
    "\"DateValue\":\"" +
    nowAsISO +
    "\"}"
*/

      val str = compact(dataJson3)

 //     logAnalyticsClient.send(str, "sparktest1", "DateValue")

      client.sendMessage(str, "DateValue")

    }

  }

}
