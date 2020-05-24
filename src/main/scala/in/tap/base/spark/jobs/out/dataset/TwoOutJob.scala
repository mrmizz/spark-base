package in.tap.base.spark.jobs.out.dataset

import in.tap.base.spark.main.OutArgs.TwoOutArgs
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode}

import scala.reflect.runtime.universe.TypeTag

trait TwoOutJob[A <: Product, B <: Product] {

  val outArgs: TwoOutArgs

  implicit val writeEncoderA: Encoder[A] = {
    Encoders.product[A]
  }

  implicit val writeEncoderB: Encoder[B] = {
    Encoders.product[B]
  }

  implicit val writeTypeTagA: TypeTag[A]

  implicit val writeTypeTagB: TypeTag[B]

  def write(ds1: Dataset[A], ds2: Dataset[B]): Unit = {
    ds1.write
      .mode(SaveMode.Overwrite)
      .format(outArgs.out1.format)
      .save(outArgs.out1.path)

    ds2.write
      .mode(SaveMode.Overwrite)
      .format(outArgs.out2.format)
      .save(outArgs.out2.path)
  }

}
