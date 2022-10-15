package com.yash.MapReduce

import com.yash.utils.{MyUtilsLib, Parameters}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.*
import org.slf4j.LoggerFactory

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

object One {
  val logger = LoggerFactory.getLogger(classOf[One.type]) //logger instantiation
  val logMsgRegexPattern = new Regex(Parameters.getLogRegex);

  val hrs: Boolean = Parameters.getHourly
  //method to run job one
  @main def runMapReduceOne(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("JobOne")
    //    conf.set("fs.defaultFS", "local")
    //    conf.set("mapreduce.job.maps", "1")
    //    conf.set("mapreduce.job.reduces", "1")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[Text])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath)) // new Path(args[0])
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    logger.info("Job One has Started")
    JobClient.runJob(conf)
    logger.info("Job One has Finished")

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, Text] :
    // creates a key based on time interval and list of integer values representing a log level and presence of regex in message 
    private val timeInterval = new Text()
    private val typeCount = new Text()

    //Mapper for part 1
    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter): Unit =

      val line: String = value.toString
      val segments = line.split(" ")
      if (MyUtilsLib.isValidLog(line) == false) {
        logger.warn("Invalid Log message in input -> " + line)
        timeInterval.set("Invalid Log Messages : ")
        typeCount.set("1")
      } else {
        timeInterval.set(MyUtilsLib.getTimeInterval(segments(0), hrs))
        typeCount.set(getComputedValue(segments(2), line.split("[$][ ][-][ ]")(1)))
      }
      logger.debug("key:Val -> { ", timeInterval.toString + ":" + typeCount.toString + " }")
      output.collect(timeInterval, typeCount)

    def getComputedValue(messageType: String, message: String): String = {
      /*returns a string containing space seperated integers representing presence of log message types and regex pattern
              eg - 0 0 1 0 1
              means log message type was info and regex was detected in the log message
      */

      val error = if (messageType == "ERROR") 1 else 0
      val warn = if (messageType == "WARN") 1 else 0
      val info = if (messageType == "INFO") 1 else 0
      val debug = if (messageType == "DEBUG") 1 else 0

      //returns 0 if no regex matching string found in log message else return 1
      val regexInLog = if (logMsgRegexPattern.findFirstIn(message).getOrElse("Not Found") == "Not Found") 0 else 1

      s"$error $warn $info $debug $regexInLog"
    }

  //reducer implementation for part 2
  class Reduce extends MapReduceBase with Reducer[Text, Text, Text, Text] : //Reducer implementation for Q1
    
    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, Text], reporter: Reporter): Unit =
      //val summary = values.asScala.reduce(sumarizeStats)
      val summary = values.asScala.foldLeft(new Text("0 0 0 0 0"))(sumarizeStats(_,_))
      output.collect(key, summary)

    // function accepts two Texts of format "error_count warn_count info_count debug_count" and returns a Text which represents column wise sum
    // eg - v1 = 0 0 1 0 1, v2= 0 1 0 0 1 output = 0 1 1 0 2 
    def sumarizeStats(valueOne: Text, valueTwo: Text): Text = {
      val oneList = valueOne.toString.split(" ")
      val twoList = valueTwo.toString.split(" ")
      new Text(oneList.zip(twoList).map((s1: String, s2: String) => {
        (s1.toInt + s2.toInt).toString
      }).mkString(" "))
    }
}
