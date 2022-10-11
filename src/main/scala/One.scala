object One {

  import org.apache.hadoop.fs.Path
  import org.apache.hadoop.conf.*
  import org.apache.hadoop.io.*
  import org.apache.hadoop.util.*
  import org.apache.hadoop.mapred.*

  import java.io.IOException
  import java.util
  import scala.jdk.CollectionConverters.*
  import org.slf4j.Logger
  import org.slf4j.LoggerFactory

  import scala.util.matching.Regex

  object One {
    val myUtils = new Utils()
    val mapLogger = LoggerFactory.getLogger(classOf[One.type]) //logger instantiation
    val logMsgRegexPattern = new Regex("([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}");
    val timeStampRegex = "\\d{2}[:]\\d{2}[:]\\d{2}[.]\\d{3}"
    val logMessageTypes = "WARN INFO DEBUG ERROR"
    val hrs = false

    class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, Text] :

      private val timeInterval = new Text()
      private val typeCount = new Text()


      def isValidLog(logMsg: String): Boolean =
        val segments = logMsg.split(" ")
        val messageContent = logMsg.split("[$][ ][-][ ]")

        segments(0).matches(timeStampRegex) && ((segments(2).length == 4) || (segments(2).length == 5)) && logMessageTypes.contains(segments(2)) && messageContent.length == 2

      def getTimeInterval(time: String, hrs: Boolean): String =
        try {
          if (hrs == false) time.substring(0, 5) + ":00.0 - " + time.substring(0, 5) + ":59.9" else time.substring(0, 2) + ":00:00 - " + time.substring(0, 2) + ":59:59"
        } catch {
          case e: Exception => "InvalidTime"
        }

      def getComputedValue(messageType: String, message: String): String = {
        val error = if (messageType == "ERROR") 1 else 0
        val warn = if (messageType == "WARN") 1 else 0
        val info = if (messageType == "INFO") 1 else 0
        val debug = if (messageType == "DEBUG") 1 else 0

        //returns 0 if no regex matching string found in log message else return 1
        val regexInLog = if (logMsgRegexPattern.findFirstIn(message).getOrElse("Not Found") == "Not Found") 0 else 1

        s"$error $warn $info $debug $regexInLog"
      }

      @throws[IOException]
      def map(key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter): Unit =
        val line: String = value.toString
        val segments = line.split(" ")
        if (isValidLog(line) == false) {
          mapLogger.warn("Invalid Log message in input -> " + line)
          timeInterval.set("Invalid Log Messages : ")
          typeCount.set("1")
        } else {
          timeInterval.set(getTimeInterval(segments(0), hrs))
          typeCount.set(getComputedValue(segments(2), line.split("[$][ ][-][ ]")(1)))
        }
        mapLogger.debug("key:Val -> {}", timeInterval.toString + ":" + typeCount.toString)
        output.collect(timeInterval, typeCount)


    //reducer implementation for Q1
    class Reduce extends MapReduceBase with Reducer[Text, Text, Text, Text] : //Reducer implementation for Q1
      // function accepts two Texts of format "error_count warn_count info_count debug_count" and returns a Text which represents column wise sum
      def sumarizeStats(valueOne: Text, valueTwo: Text): Text = {
        val oneList = valueOne.toString.split(" ")
        val twoList = valueTwo.toString.split(" ")
        new Text(oneList.zip(twoList).map((s1: String, s2: String) => {
          (s1.toInt + s2.toInt).toString
        }).mkString(" "))
      }

      override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, Text], reporter: Reporter): Unit =
        val summary = values.asScala.reduce(sumarizeStats)
        output.collect(key, summary)


    @main def runMapReduce(inputPath: String, outputPath: String) =
      val conf: JobConf = new JobConf(this.getClass)
      conf.setJobName("WordCount")
      conf.set("fs.defaultFS", "local")

      conf.set("mapreduce.job.maps", "1")
      conf.set("mapreduce.job.reduces", "1")
      conf.setOutputKeyClass(classOf[Text])
      conf.setOutputValueClass(classOf[Text])
      conf.setMapperClass(classOf[Map])
      conf.setCombinerClass(classOf[Reduce])
      conf.setReducerClass(classOf[Reduce])
      conf.setInputFormat(classOf[TextInputFormat])
      conf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])
      FileInputFormat.setInputPaths(conf, new Path(inputPath)) // new Path(args[0])
      FileOutputFormat.setOutputPath(conf, new Path(outputPath))
      JobClient.runJob(conf)
    //log info - job started
  }

}
