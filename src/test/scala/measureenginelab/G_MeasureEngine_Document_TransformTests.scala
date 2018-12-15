package measureenginelab

import measureenginelab.F_MeasureEngine_Document.{InputModel, InputProcedure}
import measureenginelab.TestData._
import org.apache.spark.sql
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class G_MeasureEngine_Document_TransformTests extends FlatSpec with TestSparkSession {

  implicit val spark: sql.SparkSession = sparkSession
  import spark.implicits._

  behavior of "transform"

  it should "shape data as expected" in {
    val encounters = mkDataFrame(TestEncounter("001"), TestEncounter("002"), TestEncounter("003"))
    val procedures = mkDataFrame(
      TestProcedure("001", "300.1", "ICD-9-PCS"),
      TestProcedure("001", "400.8", "ICD-9-PCS"),
      TestProcedure("003", "V30.5", "ICD-9-PCS"))

    val results = F_MeasureEngine_Document.transform(encounters, procedures)

    results.collect() should contain theSameElementsAs Seq(
      InputModel("001", Seq(InputProcedure("300.1", "ICD-9-PCS"), InputProcedure("400.8", "ICD-9-PCS"))),
      InputModel("002", null),
      InputModel("003", Seq(InputProcedure("V30.5", "ICD-9-PCS"))))
  }
}

case class TestEncounter(encounter_id: String = null)

case class TestProcedure(encounter_id: String = null, code: String = null, code_system: String = null)
