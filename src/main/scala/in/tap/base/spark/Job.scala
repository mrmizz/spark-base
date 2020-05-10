package in.tap.base.spark

import org.apache.spark.sql.SparkSession

trait Job {

  def run()(implicit spark: SparkSession): Unit

}
