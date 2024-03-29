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

object Four {
  val logger = LoggerFactory.getLogger(classOf[Four.type]) //logger instantiation
  val logMsgRegexPattern = new Regex(Parameters.getLogRegex) //new Regex("([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}");
  
  @main def runMapReduceFour(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("JobFour")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath)) // new Path(args[0])
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
    logger.info("Job Four has started")

  //Produces K:V pairs of form logType: message length if it contains regex pattern else 0
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] : //mapper implementation for Q1

    private val logType = new Text()
    private val messageLen = new IntWritable(0)

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line = value.toString
      val segments = line.split(" ")
      if (MyUtilsLib.isValidLog(line) == true) {
        logType.set(segments(2))
        messageLen.set(getLength(line.split("[$][ ][-][ ]")(1)))
      } else {
        logger.warn("Invalid Log message in input -> " + line)
        logType.set("Invalid Log Messages : ")
      }

      logger.debug("Message Type : Length of regex containing string -> " + logType.toString + " : " + messageLen.get)
      output.collect(logType, new IntWritable(messageLen.get))

    def getLength(message: String): Int = {
      if (logMsgRegexPattern.findFirstIn(message).getOrElse("Not Found") == "Not Found") 0 else message.length()
    }

  //reducer implementation for Q4
  //for given log type reduces to max among two lenghts 
  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] : //Reducer implementation for Q1

    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = if (key.toString == "Invalid Log Messages : ") new IntWritable(-1) else new IntWritable(values.asScala.foldLeft(-1)(_ max _.get)) //reduce((val1, val2) => new IntWritable(val1.get() max val2.get()))
      output.collect(key, new IntWritable(sum.get()))
}
