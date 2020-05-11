package in.tap.base.spark.jobs.in

import in.tap.base.spark.main.InArgs.OneInArgs
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import scala.reflect.runtime.universe.TypeTag

trait OneInJob[A <: Product] {

  val inArgs: OneInArgs

  implicit val encoder: Encoder[A] = {
    Encoders.product[A]
  }

  implicit val typeTag: TypeTag[A]

  def read(implicit spark: SparkSession): Dataset[A] = {
    spark.read
      .format(inArgs.in1.format)
      .load(inArgs.in1.path)
      .as[A]
  }

}
