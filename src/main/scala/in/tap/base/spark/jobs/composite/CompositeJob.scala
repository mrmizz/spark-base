package in.tap.base.spark.jobs.composite

import in.tap.base.spark.main.{InArgs, OutArgs}
import org.apache.spark.sql.SparkSession

abstract class CompositeJob(inArgs: InArgs, outArgs: OutArgs)(implicit spark: SparkSession) {

  def execute(): Unit

}
