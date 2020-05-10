package in.tap.base.spark.jobs.composite

import in.tap.base.spark.jobs.in.OneInJob
import in.tap.base.spark.jobs.out.TwoOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.TwoOutArgs
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

abstract class OneInTwoOutJob[A, B, C](inArgs: OneInArgs, outArgs: TwoOutArgs)(implicit encoder: Encoder[A])
    extends CompositeJob(inArgs, outArgs)
    with OneInJob[A]
    with TwoOutJob[B, C] {

  def transform(input: Dataset[A]): (Dataset[B], Dataset[C])

  override final def execute(implicit spark: SparkSession): Unit = {
    val (ds1: Dataset[B], ds2: Dataset[C]) = transform(read)
    write(ds1, ds2)
  }

}
