package in.tap.base.spark.populator

import in.tap.base.spark.jobs.composite.CompositeJob
import in.tap.base.spark.jobs.in.dataset.OneInJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe

/**
  * Populate your Dynamo DB table.
  * Make sure your AWS EMR Cluster is configured
  * with an IAM Role with necessary permissions.
  *
  * @param inArgs s3 path to source data
  * @param outArgs dynamo db table name
  * @tparam A input type
  */
class DynamoPopulator[A <: Product](val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[A]
) extends CompositeJob(inArgs, outArgs)
    with OneInJob[A] {

  override def execute(): Unit = {
    read.write
      .option("region", "us-west-2")
      .option("tableName", outArgs.out1.path)
      .format("dynamodb")
      .save()
  }

}
