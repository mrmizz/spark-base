package in.tap.base.spark.jobs.in.dataset

import in.tap.base.spark.main.InArgs.ThreeInArgs
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import scala.reflect.runtime.universe.TypeTag

trait ThreeInJob[A <: Product, B <: Product, C <: Product] {

  val inArgs: ThreeInArgs

  implicit val readEncoderA: Encoder[A] = {
    Encoders.product[A]
  }

  implicit val readEncoderB: Encoder[B] = {
    Encoders.product[B]
  }

  implicit val readEncoderC: Encoder[C] = {
    Encoders.product[C]
  }

  implicit val readTypeTagA: TypeTag[A]

  implicit val readTypeTagB: TypeTag[B]

  implicit val readTypeTagC: TypeTag[C]

  def read(
    implicit spark: SparkSession
  ): (Dataset[A], Dataset[B], Dataset[C]) = {
    val dsA: Dataset[A] = {
      spark.read
        .format(inArgs.in1.format)
        .load(inArgs.in1.path)
        .as[A]
    }

    val dsB: Dataset[B] = {
      spark.read
        .format(inArgs.in2.format)
        .load(inArgs.in2.path)
        .as[B]
    }

    val dsC: Dataset[C] = {
      spark.read
        .format(inArgs.in3.format)
        .load(inArgs.in3.path)
        .as[C]
    }

    (dsA, dsB, dsC)
  }

}
