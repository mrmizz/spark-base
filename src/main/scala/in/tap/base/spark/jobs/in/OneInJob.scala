package in.tap.base.spark.jobs.in

import in.tap.base.spark.main.InArgs.OneInArgs
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

trait OneInJob[A] {

  val inArgs: OneInArgs

  def read(implicit spark: SparkSession, encoder: Encoder[A]): Dataset[A] = {
    spark.read
      .format(inArgs.in1.format)
      .load(inArgs.in1.path)
      .as[A]
  }

}
