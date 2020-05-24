package in.tap.base.spark.jobs.in.dataset

import in.tap.base.spark.main.InArgs.OneInArgs
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import scala.reflect.runtime.universe.TypeTag

trait OneInJob[A <: Product] {

  val inArgs: OneInArgs

  implicit val readEncoderA: Encoder[A] = {
    Encoders.product[A]
  }

  implicit val readTypeTagA: TypeTag[A]

  def read(implicit spark: SparkSession): Dataset[A] = {
    spark.read
      .format(inArgs.in1.format)
      .load(inArgs.in1.path)
      .as[A]
  }

}
