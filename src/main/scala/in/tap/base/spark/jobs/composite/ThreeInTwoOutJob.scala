package in.tap.base.spark.jobs.composite

import in.tap.base.spark.jobs.in.ThreeInJob
import in.tap.base.spark.jobs.out.TwoOutJob
import in.tap.base.spark.main.InArgs.ThreeInArgs
import in.tap.base.spark.main.OutArgs.TwoOutArgs
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

abstract class ThreeInTwoOutJob[A, B, C, D, E](inArgs: ThreeInArgs, outArgs: TwoOutArgs)(
  implicit
  encoderA: Encoder[A],
  encoderB: Encoder[B],
  encoderC: Encoder[C]
) extends CompositeJob(inArgs, outArgs)
    with ThreeInJob[A, B, C]
    with TwoOutJob[D, E] {

  def transform(input: (Dataset[A], Dataset[B], Dataset[C])): (Dataset[D], Dataset[E])

  override final def execute(implicit spark: SparkSession): Unit = {
    val (ds1: Dataset[D], ds2: Dataset[E]) = transform(read)
    write(ds1, ds2)
  }

}
