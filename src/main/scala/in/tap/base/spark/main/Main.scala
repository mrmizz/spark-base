package in.tap.base.spark.main

import in.tap.base.spark.Job
import in.tap.base.spark.main.Args._
import org.apache.spark.sql.SparkSession

trait Main {

  def job(
    oneInOneOutArgs: OneInOneOutArgs,
    twoInOneOutArgs: TwoInOneOutArgs,
    threeInOneOutArgs: ThreeInOneOutArgs,
    step: String
  ): Job

  def main(args: Array[String]): Unit = {
    val parser: ArgsParser = {
      new ArgsParser(args)
    }

    if (args.length == 0 || args(0) == "--help") {
      parser.printHelp()
    }

    implicit val spark: SparkSession = {
      SparkSession.builder
        .appName("Wiki")
        .getOrCreate()
    }

    lazy val oneIn: OneInOneOutArgs = {
      OneInOneOutArgs(in1 = parser.in1(), out = parser.out())
    }

    lazy val twoIn: TwoInOneOutArgs = {
      TwoInOneOutArgs(in1 = parser.in1(), in2 = parser.in2(), out = parser.out())
    }

    lazy val threeIn: ThreeInOneOutArgs = {
      ThreeInOneOutArgs(in1 = parser.in1(), in2 = parser.in2(), in3 = parser.in3(), out = parser.out())
    }

    job(oneIn, twoIn, threeIn, parser.step()).run()
  }

}
