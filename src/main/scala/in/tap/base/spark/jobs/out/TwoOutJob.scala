package in.tap.base.spark.jobs.out

import in.tap.base.spark.main.OutArgs.TwoOutArgs
import org.apache.spark.sql.{Dataset, SaveMode}

trait TwoOutJob[A, B] {

  val outArgs: TwoOutArgs

  def write(ds1: Dataset[A], ds2: Dataset[B]): Unit = {
    ds1.write
      .mode(SaveMode.Overwrite)
      .format(outArgs.out1.format)
      .save(outArgs.out1.path)

    ds2.write
      .mode(SaveMode.Overwrite)
      .format(outArgs.out2.format)
      .save(outArgs.out2.path)
  }

}
