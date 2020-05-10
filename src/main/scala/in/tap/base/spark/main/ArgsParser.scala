package in.tap.base.spark.main

import org.rogach.scallop.{ScallopConf, ScallopOption}

class ArgsParser(arguments: Seq[String]) extends ScallopConf(arguments) {

  val step: ScallopOption[String] = {
    opt[String](
      name = "step",
      required = true,
      descr = "Job Name."
    )
  }

  val in1: ScallopOption[String] = {
    opt[String](
      name = "in1",
      required = true,
      descr = "Path to Resource 1."
    )
  }

  val in1Format: ScallopOption[String] = {
    opt[String](
      name = "in1-format",
      required = true,
      descr = "File Format of In Resource 1."
    )
  }

  val in2: ScallopOption[String] = {
    opt[String](
      name = "in2",
      required = false,
      descr = "Path to Resource 2."
    )
  }

  val in2Format: ScallopOption[String] = {
    opt[String](
      name = "in2-format",
      required = false,
      descr = "File Format of In Resource 2."
    )
  }

  val in3: ScallopOption[String] = {
    opt[String](
      name = "in3",
      required = false,
      descr = "Path to Resource 3."
    )
  }

  val in3Format: ScallopOption[String] = {
    opt[String](
      name = "in3-format",
      required = false,
      descr = "File Format of In Resource 3."
    )
  }

  val out1: ScallopOption[String] = {
    opt[String](
      name = "out1",
      required = true,
      descr = "Out Path 1."
    )
  }

  val out1Format: ScallopOption[String] = {
    opt[String](
      name = "out1-format",
      required = true,
      descr = "File Format of Out Resource 1."
    )
  }

  val out2: ScallopOption[String] = {
    opt[String](
      name = "out2",
      required = false,
      descr = "Out Path 2."
    )
  }

  val out2Format: ScallopOption[String] = {
    opt[String](
      name = "out2-format",
      required = false,
      descr = "File Format of Out Resource 2."
    )
  }

  verify()
}
