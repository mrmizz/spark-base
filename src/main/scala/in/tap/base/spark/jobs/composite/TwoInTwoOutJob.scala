package in.tap.base.spark.jobs.composite

import in.tap.base.spark.jobs.in.TwoInJob
import in.tap.base.spark.jobs.out.TwoOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.TwoOutArgs
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

abstract class TwoInTwoOutJob[A, B, C, D](inArgs: TwoInArgs, outArgs: TwoOutArgs)(
  implicit
  encoderA: Encoder[A],
  encoderB: Encoder[B]
) extends CompositeJob(inArgs, outArgs)
    with TwoInJob[A, B]
    with TwoOutJob[C, D] {

  def transform(input: (Dataset[A], Dataset[B])): (Dataset[C], Dataset[D])

  override final def execute(implicit spark: SparkSession): Unit = {
    val (ds1: Dataset[C], ds2: Dataset[D]) = transform(read)
    write(ds1, ds2)
  }

}
