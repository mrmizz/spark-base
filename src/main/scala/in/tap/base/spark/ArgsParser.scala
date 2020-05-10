package in.tap.base.spark

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

  val in2: ScallopOption[String] = {
    opt[String](
      name = "in2",
      required = false,
      descr = "Path to Resource 2."
    )
  }

  val in3: ScallopOption[String] = {
    opt[String](
      name = "in3",
      required = false,
      descr = "Path to Resource 3."
    )
  }

  val out: ScallopOption[String] = {
    opt[String](
      name = "out",
      required = true,
      descr = "Out Path."
    )
  }

  verify()
}
