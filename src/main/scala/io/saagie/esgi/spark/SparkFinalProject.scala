package io.saagie.esgi.spark


object SparkFinalProject {


  def main(args: Array[String]) {
    println(s"Sleeping for ${args(0)}")

    Thread.sleep(args(0).toLong)
  }

}
