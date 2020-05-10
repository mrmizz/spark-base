package in.tap.base.spark.jobs.out

import in.tap.base.spark.main.OutArgs.OneOutArgs
import org.apache.spark.sql.{Dataset, SaveMode}

trait OneOutJob[A] {

  val outArgs: OneOutArgs

  def write(ds: Dataset[A]): Unit = {
    ds.write
      .mode(SaveMode.Overwrite)
      .format(outArgs.out1.format)
      .save(outArgs.out1.path)
  }

}
