package in.tap.base.spark.jobs.composite

import in.tap.base.spark.jobs.in.TwoInJob
import in.tap.base.spark.jobs.out.OneOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

abstract class TwoInOneOutJob[A, B, C](inArgs: TwoInArgs, outArgs: OneOutArgs)(
  implicit
  encoderA: Encoder[A],
  encoderB: Encoder[B],
  spark: SparkSession
) extends CompositeJob(inArgs, outArgs)
    with TwoInJob[A, B]
    with OneOutJob[C] {

  def transform(input: (Dataset[A], Dataset[B])): Dataset[C]

  override final def execute(): Unit = {
    write(transform(read))
  }

}
