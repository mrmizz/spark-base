package in.tap.base.spark.jobs.composite

import in.tap.base.spark.jobs.in.dataset.ThreeInJob
import in.tap.base.spark.jobs.out.dataset.OneOutJob
import in.tap.base.spark.main.InArgs.ThreeInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import org.apache.spark.sql.{Dataset, SparkSession}

abstract class ThreeInOnOutJob[A <: Product, B <: Product, C <: Product, D <: Product](
  inArgs: ThreeInArgs,
  outArgs: OneOutArgs
)(
  implicit spark: SparkSession
) extends CompositeJob(inArgs, outArgs)
    with ThreeInJob[A, B, C]
    with OneOutJob[D] {

  def transform(input: (Dataset[A], Dataset[B], Dataset[C])): Dataset[D]

  override final def execute(): Unit = {
    write(transform(read))
  }

}
