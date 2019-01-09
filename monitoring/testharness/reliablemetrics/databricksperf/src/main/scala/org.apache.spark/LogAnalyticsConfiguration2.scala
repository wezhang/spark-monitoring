package org.apache.spark

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils.secrets
import org.apache.spark.internal.Logging

 /*trait LogAnalyticsConfiguration extends Logging {
  protected def getWorkspaceId: Option[String]

  protected def getSecret: Option[String]

  protected def getLogType: String

  protected def getTimestampFieldName: Option[String]

  protected val secretScopeAndKeyValidation = "^([a-zA-Z0-9_\\.-]{1,128})\\:([a-zA-Z0-9_\\.-]{1,128})$"
    .r("scope", "key")

  val workspaceId: String = {
    val value = getWorkspaceId
    val finalValue = value match {
      case Some(scopeAndKey) => {
        secretScopeAndKeyValidation.findFirstMatchIn(scopeAndKey) match {
          case Some(x) => {
            secrets.get(x.group("scope"), x.group("key"))
          }
          case None => scopeAndKey
        }
      }
      case None => throw new SparkException(s"A Log Analytics Workspace ID is required")
    }
    logInfo(s"Setting workspaceId to $finalValue")
    finalValue
  }

  val secret: String = {
    val value = getSecret
    val finalValue = value match {
      case Some(scopeAndKey) => {
        secretScopeAndKeyValidation.findFirstMatchIn(scopeAndKey) match {
          case Some(x) => {
            secrets.get(x.group("scope"), x.group("key"))
          }
          case None => scopeAndKey
        }
      }
      case None => throw new SparkException(s"A Log Analytics Secret is required")
    }
    logInfo(s"Setting workspace key to $finalValue")
    finalValue
  }


  val logType: String = {
    val value = getLogType
    logInfo(s"Setting logType to $value")
    value
  }

  val timestampFieldName: String = {
    val value = getTimestampFieldName
    logInfo(s"Setting timestampNameField to $value")
    value.orNull
  }
}*/


object LogAnalyticsListenerConfiguration2 {
  private val CONFIG_PREFIX = "spark.logAnalytics"

  private[spark] val WORKSPACE_ID = CONFIG_PREFIX + ".workspaceId"

  // We'll name this secret so Spark will redact it.
  private[spark] val SECRET = CONFIG_PREFIX + ".secret"

  private[spark] val LOG_TYPE = CONFIG_PREFIX + ".logType"

  private[spark] val DEFAULT_LOG_TYPE = "SparkListenerEvent"

  private[spark] val TIMESTAMP_FIELD_NAME = CONFIG_PREFIX + ".timestampFieldName"

  private[spark] val LOG_BLOCK_UPDATES = CONFIG_PREFIX + ".logBlockUpdates"

  private[spark] val DEFAULT_LOG_BLOCK_UPDATES = false
}

class LogAnalyticsListenerConfiguration2(sparkConf: SparkConf)
  /*extends LogAnalyticsConfiguration*/ {

  val CONFIG_PREFIX = "spark.logAnalytics"

  val WORKSPACE_ID = CONFIG_PREFIX + ".workspaceId"

  // We'll name this secret so Spark will redact it.
  val SECRET = CONFIG_PREFIX + ".secret"

  val LOG_TYPE = CONFIG_PREFIX + ".logType"

  val DEFAULT_LOG_TYPE = "SparkListenerEvent"

  val TIMESTAMP_FIELD_NAME = CONFIG_PREFIX + ".timestampFieldName"

  val LOG_BLOCK_UPDATES = CONFIG_PREFIX + ".logBlockUpdates"

  val DEFAULT_LOG_BLOCK_UPDATES = false

  import LogAnalyticsListenerConfiguration2._

  def getWorkspaceId: Option[String] = sparkConf.getOption(WORKSPACE_ID)

  def getSecret: Option[String] = sparkConf.getOption(SECRET)

  def getLogType: String = sparkConf.get(LOG_TYPE, DEFAULT_LOG_TYPE)

  def getTimestampFieldName: Option[String] = sparkConf.getOption(TIMESTAMP_FIELD_NAME)

  def logBlockUpdates: Boolean = {
    val value = sparkConf.getBoolean(LOG_BLOCK_UPDATES, DEFAULT_LOG_BLOCK_UPDATES)
  //  logInfo(s"Setting logBlockUpdates to $value")
    value
  }

 // def getWorkspaceId: Option[String]

 // def getSecret: Option[String]

 // def getLogType: String

//  def getTimestampFieldName: Option[String]

  val secretScopeAndKeyValidation = "^([a-zA-Z0-9_\\.-]{1,128})\\:([a-zA-Z0-9_\\.-]{1,128})$"
    .r("scope", "key")

  val workspaceId: String = {
    val value = getWorkspaceId
    val finalValue = value match {
      case Some(scopeAndKey) => {
        secretScopeAndKeyValidation.findFirstMatchIn(scopeAndKey) match {
          case Some(x) => {
            secrets.get(x.group("scope"), x.group("key"))
          }
          case None => scopeAndKey
        }
      }
      case None => throw new SparkException(s"A Log Analytics Workspace ID is required")
    }
    //logInfo(s"Setting workspaceId to $finalValue")
    finalValue
  }

  val secret: String = {
    val value = getSecret
    val finalValue = value match {
      case Some(scopeAndKey) => {
        secretScopeAndKeyValidation.findFirstMatchIn(scopeAndKey) match {
          case Some(x) => {
            secrets.get(x.group("scope"), x.group("key"))
          }
          case None => scopeAndKey
        }
      }
      case None => throw new SparkException(s"A Log Analytics Secret is required")
    }
    //logInfo(s"Setting workspace key to $finalValue")
    finalValue
  }


  val logType: String = {
    val value = getLogType
//    logInfo(s"Setting logType to $value")
    value
  }

  val timestampFieldName: String = {
    val value = getTimestampFieldName
 //   logInfo(s"Setting timestampNameField to $value")
    value.orNull
  }







}
