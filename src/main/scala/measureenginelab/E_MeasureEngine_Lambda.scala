package measureenginelab

import java.sql.Timestamp

import measureenginelab.Helpers._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

object E_MeasureEngine_Lambda {

  case class InputModel(encounter_id: String, admission_datetime: Timestamp, discharge_datetime: Timestamp)
  case class OutputModel(encounter_id: String, los: Int)

  def main(args: Array[String]): Unit = {
    driver()
  }

  def driver(): Unit = {
    val outputUrl = "s3a://datalake.measure-engine-lab.e1.102d.nonphi/dev/enriched/los"

    using(createSession())(spark => {

      import spark.implicits._

      spark.catalog.setCurrentDatabase("measureenginelab_dev_current")
      val encounters = spark.table("encounters_patient_billing")

      val input = transform(encounters)
      val output = input.map(calculate)

      output.write.mode(SaveMode.Overwrite).parquet(outputUrl)
      spark.sql(
        s"""
           |create external table if not exists measureenginelab_dev_enriched.los (
           |    encounter_id string,
           |    los int
           |) stored as parquet
           |location "$outputUrl"
         """.stripMargin)
    })
  }

  def transform(encounters: DataFrame): Dataset[InputModel] = {
    import encounters.sparkSession.implicits._

    encounters
      .select($"encounter_id", $"discharge_datetime", $"admission_datetime")
      .as[InputModel]
  }

  def calculate(input: InputModel): OutputModel = {
    import input._
    val los =
      if(admission_datetime == null || discharge_datetime == null) {
        1
      } else {
        math.max(
          1,
          (discharge_datetime.getTime - admission_datetime.getTime) / (1000L * 60L * 60L * 24L)
        )
      }

    OutputModel(encounter_id, los.asInstanceOf[Int])
  }
}
