package org.apache.spark

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.scheduler._
import org.apache.spark.streaming.scheduler.StreamingListenerEvent
import org.apache.spark.util.JsonProtocol
import org.json4s.jackson.JsonMethods.{compact, parse}

import scala.util.control.NonFatal


object SerializerUtil {

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "Event")
  private class StreamingListenerEventMixIn {}

  // Add the Event field since the StreamingListenerEvents don't extend the SparkListenerEvent trait
  mapper.addMixIn(classOf[StreamingListenerEvent], classOf[StreamingListenerEventMixIn])

   def logEvent(event: AnyRef): String={
    val s = try {

      val json = event match {
        case e: SparkListenerEvent => Some(JsonProtocol.sparkEventToJson(e))
        case e: StreamingListenerEvent => Some(parse(mapper.writeValueAsString(e)))
        case e =>
          None
      }
      if (json.isDefined) {
        Some(compact(json.get))
      } else {
        None
      }


    } catch {
      case NonFatal(e) =>
        None
    }

     s.get


  }

}
