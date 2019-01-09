package org.apache.spark.listeners

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.scheduler._
import org.apache.spark.{LogAnalytics, LogAnalyticsListenerConfiguration2, LogAnalyticsPerf, SparkConf,LogAnalyticsPerf2}

class LogAnalyticsStreamingListener(sparkConf: SparkConf) extends StreamingListener
  with Logging /*with LogAnalytics*/ {


  val config = new LogAnalyticsListenerConfiguration2(sparkConf)
  val logger = new LogAnalyticsPerf2(config)
 // val logger = new LogAnalyticsPerf()

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    logger.logEvent(streamingStarted)
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
    logger.logEvent(receiverStarted)
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
    logger.logEvent(receiverError)
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {
    logger.logEvent(receiverStopped)
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    logger.logEvent(batchSubmitted)
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    logger.logEvent(batchStarted)
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    logger.logEvent(batchCompleted)
  }

  override def onOutputOperationStarted(outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = {
    logger.logEvent(outputOperationStarted)
  }

  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {
    logger.logEvent(outputOperationCompleted)
  }
}
