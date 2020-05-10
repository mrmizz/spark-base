package in.tap.base.spark.io

object Compression {

  def apply(boolean: Boolean, format: String): String = {
    (boolean, format) match {
      case (false, _)              => "none"
      case (true, Formats.JSON)    => "gzip"
      case (true, Formats.PARQUET) => "snappy"
      case _                       => "none"
    }
  }

}
