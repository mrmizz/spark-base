package in.tap.base.spark.jobs.composite

import in.tap.base.spark.main.{InArgs, OutArgs}
import org.apache.spark.sql.SparkSession

abstract class CompositeJob(inArgs: InArgs, outArgs: OutArgs) {

  def execute(implicit spark: SparkSession): Unit

}
