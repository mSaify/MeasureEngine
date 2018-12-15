package measureenginelab

import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}

object TestData {
  def mkDataFrame[T : Encoder](entities: T*)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    entities.toDF
  }
}
