import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import org.apache.hadoop.util.*
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

object Three {
  val logger = LoggerFactory.getLogger(classOf[Three.type])

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] : //mapper implementation for Q1

    private val logType = new Text()
    private final val one = new IntWritable(1)

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val segments = value.toString.split(" ")
      if (MyUtilsLib.isValidLog(value.toString) == true){
        logType.set(segments(2))
      }
      else{
        logger.warn("Invalid Log message in input -> " + value.toString)
        logType.set("Invalid Log Messages : ")
      }

      logger.debug("Message Type : " + logType.toString)
      output.collect(logType, one)

  //reducer implementation for Q3
  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] : //Reducer implementation for Q1

    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((val1, val2) => new IntWritable(val1.get() + val2.get()))
      output.collect(key, new IntWritable(sum.get()))

  @main def runMapReduceThree(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("JobThree")
    conf.set("fs.defaultFS", "local")

    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
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
    logger.info("Job Three has started")
}
