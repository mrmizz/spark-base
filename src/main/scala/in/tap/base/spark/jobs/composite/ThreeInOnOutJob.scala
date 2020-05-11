package in.tap.base.spark.jobs.composite

import in.tap.base.spark.jobs.in.ThreeInJob
import in.tap.base.spark.jobs.out.OneOutJob
import in.tap.base.spark.main.InArgs.ThreeInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

abstract class ThreeInOnOutJob[A, B, C, D](inArgs: ThreeInArgs, outArgs: OneOutArgs)(
  implicit
  encoderA: Encoder[A],
  encoderB: Encoder[B],
  encoderC: Encoder[C],
  spark: SparkSession
) extends CompositeJob(inArgs, outArgs)
    with ThreeInJob[A, B, C]
    with OneOutJob[D] {

  def transform(input: (Dataset[A], Dataset[B], Dataset[C])): Dataset[D]

  override final def execute(): Unit = {
    write(transform(read))
  }

}
