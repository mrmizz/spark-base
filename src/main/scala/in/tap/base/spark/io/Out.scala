package in.tap.base.spark.io

final case class Out(
  path: String,
  format: String = Formats.JSON
)
