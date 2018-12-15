package measureenginelab

import measureenginelab.Helpers._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

object F_MeasureEngine_Document {

  case class InputProcedure(code: String, code_system: String)
  case class InputModel(encounter_id: String, procedures: Seq[InputProcedure])
  case class OutputModel(encounter_id: String, procedure_count: Int)

  def main(args: Array[String]): Unit = {
    driver()
  }

  def driver(): Unit = {
    val outputUrl = "s3a://datalake.measure-engine-lab.e1.102d.nonphi/dev/enriched/procedure_count"

    using(createSession())(spark => {

      import spark.implicits._

      spark.catalog.setCurrentDatabase("measureenginelab_dev_current")
      val encounters = spark.table("encounters_patient_billing")
      val procedures = spark.table("procedures_patient_billing")

      val input = transform(encounters, procedures)
      val output = input.map(calculate)

      output.write.mode(SaveMode.Overwrite).parquet(outputUrl)
      spark.sql(
        s"""
           |create external table if not exists measureenginelab_dev_enriched.procedure_count (
           |    encounter_id string,
           |    procedure_count int
           |) stored as parquet
           |location "$outputUrl"
         """.stripMargin)
    })
  }

  def transform(encounters: DataFrame, procedures: DataFrame): Dataset[InputModel] = {
    import encounters.sparkSession.implicits._

    val groupedProcedures = procedures
      .groupBy($"encounter_id")
      .agg(collect_list(struct($"code", $"code_system")) as "procedures")

    encounters
      .join(groupedProcedures, encounters("encounter_id") === groupedProcedures("encounter_id"), "left")
      .select(encounters("encounter_id"), $"procedures")
      .as[InputModel]
  }

  def calculate(input: InputModel): OutputModel = {
    import input._

    val count =
      if (procedures == null) {
        0
      } else {
        procedures.size
      }

    OutputModel(encounter_id, count)
  }
}
