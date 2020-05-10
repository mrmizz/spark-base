package in.tap.base.spark.main

import in.tap.base.spark.io.Out

sealed trait OutArgs

object OutArgs {

  final case class OneOutArgs(out1: Out) extends OutArgs

  final case class TwoOutArgs(out1: Out, out2: Out) extends OutArgs

}
