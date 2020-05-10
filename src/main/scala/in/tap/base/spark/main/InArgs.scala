package in.tap.base.spark.main

import in.tap.base.spark.io.In

sealed trait InArgs

object InArgs {

  final case class OneInArgs(in1: In) extends InArgs

  final case class TwoInArgs(in1: In, in2: In) extends InArgs

  final case class ThreeInArgs(in1: In, in2: In, in3: In) extends InArgs

}
