package measureenginelab

import measureenginelab.Helpers._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}


/*
  spark-submit is used to launch a new Spark job. Spark submit needs a "fat jar" which can be built from the CLI with:
  ./gradlew shadowJar

  The resulting jar is saved to ./build/libs/big-data-measure-engine-all.jar

  Here's the command to run this measure engine from an EC2 machine:
  /usr/lib/spark/bin/spark-submit --master yarn --verbose --deploy-mode cluster --class measureenginelab.D_MeasureEngineBasics measure-engine-lib.jar

 */

object D_MeasureEngineBasics {

  def main(args: Array[String]): Unit = {
    driver()
  }

  def driver(): Unit = {
    val outputUrl = "s3a://datalake.measure-engine-lab.e1.102d.nonphi/dev/enriched/los"

    using(createSession())(spark => {

      spark.catalog.setCurrentDatabase("measureenginelab_dev_current")
      val encounters = spark.table("encounters_patient_billing")

      val los = transform(encounters)

      los.write.mode(SaveMode.Overwrite).parquet(outputUrl)
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

  def transform(encounters: DataFrame): DataFrame = {
    import encounters.sparkSession.implicits._

    encounters
      .select(
        $"encounter_id",
        greatest(
          lit(1),
          coalesce(
            datediff($"discharge_datetime", $"admission_datetime"),
            lit(1))) as "los")

    /*
     An alternative implementation:
      encounters.selectExpr("encounter_id", "greatest(1, coalesce(datediff(discharge_datetime, admission_datetime), 1))")
     */
  }
}
