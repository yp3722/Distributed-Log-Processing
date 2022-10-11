package com.yash

import com.yash.MapReduce.{Four, One, Three}

class RunProject{

}

object RunProject {

  def run(input: String, output: String) =
    One.runMapReduceOne(input, output + "-One")
    Three.runMapReduceThree(input, output + "-Three")
    Four.runMapReduceFour(input, output + "-Four")
  //Two.runMapReduceTwo(input,output+"OutputTwo")

  def main(args: Array[String]) = {
    try {
      run(args(0), args(1))
    } catch {
      case e: Exception => System.out.println("invalid arguments supplied")
    }

  }
}
