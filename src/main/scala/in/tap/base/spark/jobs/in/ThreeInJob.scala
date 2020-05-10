package in.tap.base.spark.jobs.in

import in.tap.base.spark.main.InArgs.ThreeInArgs
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

trait ThreeInJob[A, B, C] {

  val inArgs: ThreeInArgs

  def read(
    implicit
    spark: SparkSession,
    encoderA: Encoder[A],
    encoderB: Encoder[B],
    encoderC: Encoder[C]
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
