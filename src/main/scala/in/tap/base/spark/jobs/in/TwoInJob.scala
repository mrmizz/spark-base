package in.tap.base.spark.jobs.in

import in.tap.base.spark.main.InArgs.TwoInArgs
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import scala.reflect.runtime.universe.TypeTag

trait TwoInJob[A <: Product, B <: Product] {

  val inArgs: TwoInArgs

  implicit val encoderA: Encoder[A] = {
    Encoders.product[A]
  }

  implicit val encoderB: Encoder[B] = {
    Encoders.product[B]
  }

  implicit val typeTagA: TypeTag[A]

  implicit val typeTagB: TypeTag[B]

  def read(implicit spark: SparkSession): (Dataset[A], Dataset[B]) = {
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

    dsA -> dsB
  }

}
