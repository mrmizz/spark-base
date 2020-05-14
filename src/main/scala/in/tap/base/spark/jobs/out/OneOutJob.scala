package in.tap.base.spark.jobs.out

import in.tap.base.spark.main.OutArgs.OneOutArgs
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode}

import scala.reflect.runtime.universe.TypeTag

trait OneOutJob[A <: Product] {

  val outArgs: OneOutArgs

  implicit val writeEncoderA: Encoder[A] = {
    Encoders.product[A]
  }

  implicit val writeTypeTagA: TypeTag[A]

  def write(ds: Dataset[A]): Unit = {
    ds.write
      .mode(SaveMode.Overwrite)
      .format(outArgs.out1.format)
      .save(outArgs.out1.path)
  }

}
