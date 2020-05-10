package in.tap.base.spark.io

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

final case class Read[A](path: String, format: String = Formats.JSON) {

  def apply(implicit encoder: Encoder[A], spark: SparkSession): Dataset[A] = {
    spark.read
      .format(format)
      .load(path)
      .as[A]
  }

}
