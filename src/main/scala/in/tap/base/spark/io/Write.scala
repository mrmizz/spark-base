package in.tap.base.spark.io

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

final case class Write[A](ds: Dataset[A], path: String, format: String = Formats.JSON, compression: Boolean) {

  def apply(implicit spark: SparkSession): Unit = {
    ds.write
      .mode(SaveMode.Overwrite)
      .format(format)
      .option("compression", Compression(compression, format))
      .save(path)
  }

}
