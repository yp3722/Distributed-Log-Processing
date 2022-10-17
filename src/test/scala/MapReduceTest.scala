import org.apache.hadoop.io.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.yash.utils.*
import com.yash.MapReduce.*

class MapReduceTest extends AnyFlatSpec with Matchers {
  behavior of "Utils and Reduce functions"

  it should "summarize stats for two log messages" in {
    val reduceOne = new One.Reduce()
    val t1 = new Text("12 2 0 141")
    val t2 = new Text("17 3 8 16")
    reduceOne.sumarizeStats(t1,t2).toString should equal ("29 5 8 157")
  }

  it should "identify if log message is of correct format for correct input" in {
    val ip1 = "19:45:33.069 [scala-execution-context-global-22] INFO  HelperUtils.Parameters$ - OsI1`qAeU5H;\\+"
    val op1 = MyUtilsLib.isValidLog(ip1)
    op1 should equal (true)
  }

  it should "identify if log message is of correct format for incorrect input" in {
    val ip2 = "xecution-context-global-22] DEBUG HelperUtils.Parameters$ - Jr"
    val op2 = MyUtilsLib.isValidLog(ip2)
    op2 should equal (false)
  }

  it should "return the respective 30 sec or 30 min time interval for given time interval - minute interval" in {
    val ip3 = "19:45:33.069 [scala-execution-context-global-22] INFO  HelperUtils.Parameters$ - OsI1`qAeU5H;\\+"
    val op3 = MyUtilsLib.getTimeInterval(ip3.split(" ")(0),false)
    op3 should equal ("19:45:00.0 - 19:45:59.9")
  }

  it should "return the respective 30 sec or 30 min time interval for given time interval - hour interval" in {
    val ip4 = "19:45:33.069 [scala-execution-context-global-22] INFO  HelperUtils.Parameters$ - OsI1`qAeU5H;\\+"
    val op4 = MyUtilsLib.getTimeInterval(ip4.split(" ")(0), true)
    op4 should equal ("19:00:00 - 19:59:59")
  }

  it should "identify message type and presence of regex" in {
    val mapOne = new One.Map()
    val ip5 = "19:45:33.069 [scala-execution-context-global-22] INFO  HelperUtils.Parameters$ - OsI1`qAeU5H;\\+"
    val op5 = mapOne.getComputedValue(ip5.split(" ")(2),ip5.split("[$][ ][-][ ]")(1))
    op5 should equal ("0 0 1 0 0")
  }


}
