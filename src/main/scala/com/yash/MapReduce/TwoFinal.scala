package com.yash.MapReduce

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

object TwoFinal {
  val logger = LoggerFactory.getLogger(classOf[TwoFinal.type]) //logger instantiation

  class Map extends MapReduceBase with Mapper[LongWritable, Text, IntWritable, Text] :


    //Mapper for part 1
    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[IntWritable, Text], reporter: Reporter): Unit =

      val segments = value.toString.split(",")
      val count = (-1) * segments(1).trim.toInt

      System.out.println("Yash in Mapper :"+segments(0)+" :"+count)

      val opKey = new IntWritable(count)
      val opValue = new Text(segments(0))

      logger.debug("key:Val -> { ",opKey.get(),"_:_ ",segments(0)," }")

      output.collect(opKey,opValue)


  //reducer implementation for part 2


  class Reduce extends MapReduceBase with Reducer[IntWritable, Text, Text, IntWritable] : //Reducer implementation for Q1

    override def reduce(key: IntWritable, values: util.Iterator[Text], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      System.out.println("redcer "+key.get+" ")
      val count = new IntWritable(key.get * -1)
      println(s"Key now: ${count}")
      values.asScala.foreach( v=> output.collect(v, count))



  @main def runMapReduceTwoFinal(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("JobTwoFinal")
    conf.setJarByClass(classOf[Map])
    conf.setMapperClass(classOf[Map])
    conf.setReducerClass(classOf[Reduce])
    conf.setMapOutputKeyClass(classOf[IntWritable])
    conf.setMapOutputValueClass(classOf[Text])
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
//    conf.setOutputKeyClass(classOf[Text])
//    conf.setOutputValueClass(classOf[IntWritable])
//    conf.setMapperClass(classOf[Map])
//    conf.setCombinerClass(classOf[Reduce])
//    conf.setReducerClass(classOf[Reduce])
//    conf.setInputFormat(classOf[TextInputFormat])
//    conf.setMapOutputKeyClass(classOf[IntWritable])
//    conf.setMapOutputValueClass(classOf[Text])
//    conf.setOutputFormat(classOf[TextOutputFormat[IntWritable, Text]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath)) // new Path(args[0])
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
    logger.info("Job Four Stage One has started")

}
