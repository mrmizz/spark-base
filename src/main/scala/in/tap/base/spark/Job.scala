package in.tap.base.spark

import in.tap.base.spark.main.Args
import org.apache.spark.sql.SparkSession

abstract class Job(args: Args) {

  def run()(implicit spark: SparkSession): Unit

}
