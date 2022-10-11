package com.yash.utils

import org.slf4j.LoggerFactory

object MyUtilsLib {
  val mapLogger = LoggerFactory.getLogger(classOf[MyUtilsLib.type]) //logger instantiation
  val timeStampRegex = Parameters.getTimeStampRegex
  val logMessageTypes = Parameters.getLogMsgTypes


  //function checks if input string is of correct format eg 19:45:09.170 [scala-execution-context-global-22] INFO  HelperUtils.Parameters$ - ;kNI&V%v<c#eSDK@lPY(
  def isValidLog(logMsg: String): Boolean =
    try {
      val segments = logMsg.split(" ")
      val messageContent = logMsg.split("[$][ ][-][ ]")

      segments(0).matches(timeStampRegex) && ((segments(2).length == 4) || (segments(2).length == 5)) && logMessageTypes.contains(segments(2)) && messageContent.length == 2
    }
    catch {

      case e: Exception => {
        mapLogger.error("input :" + logMsg + " does not seem to be of correct format")
        false
      }
    }

  //function extracts time from a valid log message and returns its respective time interval of size 60 secs or 60 mins
  def getTimeInterval(time: String, hrs: Boolean): String =
    try {
      if (hrs == false) time.substring(0, 5) + ":00.0 - " + time.substring(0, 5) + ":59.9" else time.substring(0, 2) + ":00:00 - " + time.substring(0, 2) + ":59:59"
    } catch {
      case e: Exception => {
        mapLogger.error("Incorrect timestamp fromat : " + time)
        "Invalid TimeStamps"
      }
    }
}
