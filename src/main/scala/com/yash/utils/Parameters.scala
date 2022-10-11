package com.yash.utils

import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

object Parameters {
  val logger = LoggerFactory.getLogger(classOf[Parameters.type]) //logger instantiation
  val defaultConfig = ConfigFactory.parseResources("default.conf");
  val config = ConfigFactory.parseResources("Override.conf").withFallback(defaultConfig).resolve()

  def getHourly: Boolean = {
    try {
      val op = config.getBoolean("StatsForLogs.hourly")
      logger.debug("Parameter Hourly : " + op)
      op
    }
    catch {
      case e: Exception => {
        false
      }
    }

  }

  def getLogRegex: String = {
    try {
      val op = config.getString("StatsForLogs.logMsgRegexPattern")
      logger.debug("Parameter LogMsgRegex : " + op)
      op
    }
    catch {
      case e: Exception => {
        "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}"
      }
    }

  }

  def getTimeStampRegex: String = {
    try {
      val op = config.getString("StatsForLogs.timeStampRegex")
      logger.debug("Parameter TimeStampRegex : " + op)
      op
    }
    catch {
      case e: Exception => {
        "\\d{2}[:]\\d{2}[:]\\d{2}[.]\\d{3}"
      }
    }

  }

  def getLogMsgTypes: String = {
    try {
      val op = config.getString("StatsForLogs.logMessageTypesList")
      logger.debug("Parameter LogMessageTypes : " + op)
      op
    }
    catch {
      case e: Exception => {
        "WARN INFO DEBUG ERROR"
      }
    }

  }
}
