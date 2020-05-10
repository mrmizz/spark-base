package in.tap.base.spark.main

sealed trait Args

object Args {

  final case class OneInOneOutArgs(
    in1: String,
    out: String
  ) extends Args

  final case class TwoInOneOutArgs(
    in1: String,
    in2: String,
    out: String
  ) extends Args

  final case class ThreeInOneOutArgs(
    in1: String,
    in2: String,
    in3: String,
    out: String
  ) extends Args

}
