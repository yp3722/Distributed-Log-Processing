package com.yash.MapReduce

import com.yash.utils.{MyUtilsLib, Parameters}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.*
import org.slf4j.LoggerFactory

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

//Produces intermidiatery results which needs to be sorted
object Two {
  val logger = LoggerFactory.getLogger(classOf[Two.type]) //logger instantiation
  val logMsgRegexPattern = new Regex(Parameters.getLogRegex);

  val hrs: Boolean = Parameters.getHourly

  //method to run jobTwo stage 1
  @main def runMapReduceTwo(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("JobTwoStageOne")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[MapStageOne])
    conf.setCombinerClass(classOf[ReduceStageOne])
    conf.setReducerClass(classOf[ReduceStageOne])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath)) // new Path(args[0])
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    logger.info("Job Two Stage One has started")
    JobClient.runJob(conf)
    logger.info("Job Two Stage One has finished")

  // creates K:V pairs based on timesinterval and Int value 0 or 1 depending on Log type = Error and regex presence in message
  class MapStageOne extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :

    private val timeInterval = new Text()
    private val typeCount = new IntWritable()

    //Mapper for part 1
    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val segments = line.split(" ")
      if (MyUtilsLib.isValidLog(line) == false) {
        logger.warn("Invalid Log message in input -> " + line)
        timeInterval.set("Invalid Log Messages , ")
        typeCount.set(0)
      } else {
        timeInterval.set(MyUtilsLib.getTimeInterval(segments(0), hrs) + ",")
        typeCount.set(getComputedValue(segments(2), line.split("[$][ ][-][ ]")(1)))
      }
      logger.debug("key:Val -> { ", timeInterval.toString + ":" + typeCount.toString + " }")
      output.collect(timeInterval, typeCount)

    def getComputedValue(messageType: String, message: String): Int = {
      val error = if (messageType == "ERROR") 1 else 0

      //returns 0 if no regex matching string found in log message else return 1
      val regexInLog = if (logMsgRegexPattern.findFirstIn(message).getOrElse("Not Found") == "Not Found") 0 else 1

      if ((error == 1) && (regexInLog == 1)) 1 else 0
    }



  //reducer implementation for part 2
  class ReduceStageOne extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] : //Reducer implementation for Q1
    def addTwoInts(a: IntWritable,b:IntWritable) : IntWritable = {
      new IntWritable(a.get+b.get)
    }
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.foldLeft(new IntWritable(0))(addTwoInts(_,_))//reduce((val1, val2) => new IntWritable(val1.get() + val2.get()))
      output.collect(key, new IntWritable(sum.get()))
}
