package in.tap.base.spark.jobs.composite

import in.tap.base.spark.jobs.in.dataset.TwoInJob
import in.tap.base.spark.jobs.out.dataset.OneOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import org.apache.spark.sql.{Dataset, SparkSession}

abstract class TwoInOneOutJob[A <: Product, B <: Product, C <: Product](inArgs: TwoInArgs, outArgs: OneOutArgs)(
  implicit spark: SparkSession
) extends CompositeJob(inArgs, outArgs)
    with TwoInJob[A, B]
    with OneOutJob[C] {

  def transform(input: (Dataset[A], Dataset[B])): Dataset[C]

  override final def execute(): Unit = {
    write(transform(read))
  }

}
