package in.tap.base.spark.jobs.out.dataframe

import in.tap.base.spark.main.OutArgs.OneOutArgs
import org.apache.spark.sql.{DataFrame, SaveMode}

trait OneOutJob {

  val outArgs: OneOutArgs

  def write(df: DataFrame): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .format(outArgs.out1.format)
      .save(outArgs.out1.path)
  }

}
