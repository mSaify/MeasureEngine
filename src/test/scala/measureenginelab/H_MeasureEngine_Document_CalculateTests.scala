package measureenginelab

import measureenginelab.F_MeasureEngine_Document.{InputModel, InputProcedure, OutputModel}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class H_MeasureEngine_Document_CalculateTests extends FlatSpec {

  behavior of "calculate"

  it should "return zero when procedures are null" in {
    val input = InputModel("005", null)

    val result = F_MeasureEngine_Document.calculate(input)

    result shouldBe OutputModel("005", 0)
  }

  it should "return 1 for one procedure" in {
    val input = InputModel("005", Seq(InputProcedure("30.5", "ICD-10-PCS")))

    val result = F_MeasureEngine_Document.calculate(input)

    result shouldBe OutputModel("005", 1)
  }

  it should "return 2 for two procedures" in {
    val input = InputModel("005",
      Seq(
        InputProcedure("30.5", "ICD-10-PCS"),
        InputProcedure("40.2", "ICD-10-PCS")))

    val result = F_MeasureEngine_Document.calculate(input)

    result shouldBe OutputModel("005", 1)
  }
}
