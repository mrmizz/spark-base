package in.tap.base.spark.io

final case class In(
  path: String,
  format: String = Formats.JSON
)
