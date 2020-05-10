package in.tap.base.spark.jobs.in

import in.tap.base.spark.main.InArgs.TwoInArgs
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

trait TwoInJob[A, B] {

  val inArgs: TwoInArgs

  def read(implicit spark: SparkSession, encoderA: Encoder[A], encoderB: Encoder[B]): (Dataset[A], Dataset[B]) = {
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
