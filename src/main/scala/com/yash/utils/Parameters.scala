package com.yash.utils

import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

object Parameters {

  // Class reads config file and provides values

  val logger = LoggerFactory.getLogger(classOf[Parameters.type]) //logger instantiation
  val defaultConfig = ConfigFactory.parseResources("default.conf");
  // Incase Override.conf is not available uses default.conf
  val config = ConfigFactory.parseResources("Override.conf").withFallback(defaultConfig).resolve()


  //sets time interval to hourly if true
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

  //get injected regex pattern in message
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
  
  //get regex to verify timestamp validity
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
  
  //get list of types of log levels
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
