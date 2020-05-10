package in.tap.base.spark.main

import in.tap.base.spark.io.{In, Out}
import in.tap.base.spark.jobs.composite.CompositeJob
import in.tap.base.spark.main.InArgs.{OneInArgs, ThreeInArgs, TwoInArgs}
import in.tap.base.spark.main.OutArgs.{OneOutArgs, TwoOutArgs}
import org.apache.spark.sql.SparkSession

trait Main {

  def job(step: String, inArgs: InArgs, outArgs: OutArgs): CompositeJob

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
        .master("local[*]")
        .getOrCreate()
    }

    val inPaths: (String, Option[String], Option[String]) = {
      (parser.in1(), parser.in2.toOption, parser.in3.toOption)
    }

    val inFormats: (String, Option[String], Option[String]) = {
      (parser.in1Format(), parser.in2Format.toOption, parser.in3Format.toOption)
    }

    val outPaths: (String, Option[String]) = {
      (parser.out1(), parser.out2.toOption)
    }

    val outFormats: (String, Option[String]) = {
      (parser.out1Format(), parser.out2Format.toOption)
    }

    val step: String = {
      parser.step()
    }

    val inArgs: InArgs = (inPaths, inFormats) match {
      case ((in1, Some(in2), Some(in3)), (f1, Some(f2), Some(f3))) => ThreeInArgs(In(in1, f1), In(in2, f2), In(in3, f3))
      case ((_, Some(_), Some(_)), _)                              => throw missingFormatError
      case ((in1, Some(in2), _), (f1, Some(f2), _))                => TwoInArgs(In(in1, f1), In(in2, f2))
      case ((_, Some(_), _), (_, None, _))                         => throw missingFormatError
      case ((in1, None, None), (f1, None, None))                   => OneInArgs(In(in1, f1))
    }

    val outArgs: OutArgs = (outPaths, outFormats) match {
      case ((out1, Some(out2)), (f1, Some(f2))) => TwoOutArgs(Out(out1, f1), Out(out2, f2))
      case ((_, Some(_)), _)                    => throw missingFormatError
      case ((out1, None), (f1, _))              => OneOutArgs(Out(out1, f1))
    }

    job(step, inArgs, outArgs).execute
  }

  private lazy val missingFormatError: MatchError = {
    new MatchError("A Format is required for Each Path.")
  }

}
