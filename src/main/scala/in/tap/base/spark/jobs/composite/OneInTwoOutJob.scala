package in.tap.base.spark.jobs.composite

import in.tap.base.spark.jobs.in.OneInJob
import in.tap.base.spark.jobs.out.TwoOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.TwoOutArgs
import org.apache.spark.sql.{Dataset, SparkSession}

abstract class OneInTwoOutJob[A <: Product, B <: Product, C <: Product](inArgs: OneInArgs, outArgs: TwoOutArgs)(
  implicit spark: SparkSession
) extends CompositeJob(inArgs, outArgs)
    with OneInJob[A]
    with TwoOutJob[B, C] {

  def transform(input: Dataset[A]): (Dataset[B], Dataset[C])

  override final def execute(): Unit = {
    val (ds1: Dataset[B], ds2: Dataset[C]) = transform(read)
    write(ds1, ds2)
  }

}
