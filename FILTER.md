# Experiments in Filtering

## Introduction

This branch contains experimental code changes to limit the volume of data sent to Azure Monitor. The simple changes require rebuilding the jar files for the project, replacing the jar files and/or the spark-monitoring.sh script in dbfs, and restarting the cluster in order to pick up changes.  You should only attempt to use these changes if you are comfortable changing and testing Scala code, bash scripts, and configuring log4j.

## Limiting events in SparkListenerEvent_CL

The code at [LogAnalyticsListenerSink.scala#27](src/spark-listeners-loganalytics/src/main/scala/org/apache/spark/listeners/sink/loganalytics/LogAnalyticsListenerSink.scala#27) was changed to extract the name of the event and only send it to Azure Monitor if the event name matches one of two values:

```scala
val event = (j \ "Event").extract[String]
if(event=="SparkListenerJobStart" || event=="SparkListenerJobEnd")
{
    val jsonString = compact(j)
    logDebug(s"Sending event to Log Analytics: ${jsonString}")
    logAnalyticsBufferedClient.sendMessage(jsonString, "SparkEventTime")
}
```

The above will only log events that are of type **SparkListenerJobStart** or **SparkListenerJobEnd**.

### Finding Event Names in Azure Monitor

The following query will show counts by day for all events that have been logged to Azure Monitor:
```kusto
SparkListenerEvent_CL
| project TimeGenerated, Event_s
| summarize Count=count() by tostring(Event_s), bin(TimeGenerated, 1d)
```

### Events Noted in SparkListenerEvent_CL

* SparkListenerExecutorAdded
* SparkListenerBlockManagerAdded
* org.apache.spark.sql.streaming.StreamingQueryListener$QueryStartedEvent
* org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
* org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
* org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates
* org.apache.spark.sql.streaming.StreamingQueryListener$QueryTerminatedEvent
* SparkListenerJobStart
* SparkListenerStageSubmitted
* SparkListenerTaskStart
* SparkListenerTaskEnd
* org.apache.spark.sql.streaming.StreamingQueryListener$QueryProgressEvent
* SparkListenerStageCompleted
* SparkListenerJobEnd

## Limiting Metrics in SparkMetric_CL

The code at [LogAnalyticsReporter.scala#38](src/spark-listeners-loganalytics/src/main/scala/org/apache/spark/metrics/sink/loganalytics/LogAnalyticsReporter.scala#38) was updated to replace the default `MetricFilter.All` with an inline override that includes the following filter:

```scala
private var filter = new MetricFilter() {
    override def matches(name: String, metric: Metric): Boolean = {
        name.startsWith("jvm.")
    }
}
```

The above will only include metrics where the name starts with **"jvm."**.

The supported versions of Spark use version **3.x** of ***com.codahale.metrics***, in the future with version **4.x** there are some shortcuts you can use with additional static filters for startsWith, endsWith, and contains.

### Finding Metric Names in Azure Monitor

Query to find all metric prefixes and counts by day:

```kusto
SparkMetric_CL
| project nameprefix=split(tostring(name_s),".")[0], TimeGenerated
| summarize Count=count() by tostring(nameprefix), bin(TimeGenerated, 1d)
```
If you want to get more granular, the full names can be seen with the following query. Note: This will include a large number of metrics including for specific Spark applications.

```kusto
SparkMetric_CL
| project name_s, TimeGenerated
| summarize Count=count() by tostring(name_s), bin(TimeGenerated, 1d)
```

### Metric Name Prefixes Noted in SparkMetric_CL

* jvm
* worker
* Databricks
* HiveExternalCatalog
* CodeGenerator
* application
* master
* app-20201014133042-0000 - Note: This prefix includes all metrics for a specific Spark application run.
* shuffleService
* SparkStatusTracker

## Limiting Logs in SparkLoggingEvent_CL

The logs that propagate to SparkLoggingEvent_CL do so through a log4j appender.  This can be configured by altering the spark-monitoring.sh script that is responsible for writing the log4j.properties file. The script at [spark-monitoring.sh#88](src/spark-listeners/scripts/spark-monitoring.sh#88) has been modified to set the threshold for events to be forwarded to **ERROR**.

```bash
# logAnalytics
log4j.appender.logAnalyticsAppender=com.microsoft.pnp.logging.loganalytics.LogAnalyticsAppender
log4j.appender.logAnalyticsAppender.filter.spark=com.microsoft.pnp.logging.SparkPropertyEnricher
log4j.appender.logAnalyticsAppender.Threshold=ERROR
```
