package in.tap.base.spark.jobs.composite

import in.tap.base.spark.jobs.in.ThreeInJob
import in.tap.base.spark.jobs.out.TwoOutJob
import in.tap.base.spark.main.InArgs.ThreeInArgs
import in.tap.base.spark.main.OutArgs.TwoOutArgs
import org.apache.spark.sql.{Dataset, SparkSession}

abstract class ThreeInTwoOutJob[A <: Product, B <: Product, C <: Product, D, E](
  inArgs: ThreeInArgs,
  outArgs: TwoOutArgs
)(
  implicit spark: SparkSession
) extends CompositeJob(inArgs, outArgs)
    with ThreeInJob[A, B, C]
    with TwoOutJob[D, E] {

  def transform(input: (Dataset[A], Dataset[B], Dataset[C])): (Dataset[D], Dataset[E])

  override final def execute(): Unit = {
    val (ds1: Dataset[D], ds2: Dataset[E]) = transform(read)
    write(ds1, ds2)
  }

}
