package measureenginelab

import org.apache.spark.sql.SparkSession
import org.scalatest._

object TestSparkSession {
  def createSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[2]")
      .appName("Test Suite")
      .config("spark.ui.enabled", value = false)
      .config("spark.driver.bindAddress", value = "127.0.0.1")
      .config("spark.executor.memory", "5000mb")
      .getOrCreate()
  }
}

trait TestSparkSession extends BeforeAndAfterAll { this: Suite =>

  private var sparkSessionInternal: SparkSession = _

  implicit protected def sparkSession: SparkSession = {
    if (sparkSessionInternal == null) {
      createSession()
    }
    sparkSessionInternal
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally cleanupSession()
  }

  private def cleanupSession(): Unit = {
    if(sparkSessionInternal != null) {
      sparkSessionInternal.close()
      sparkSessionInternal = null
    }
  }

  private def createSession(): Unit = {
    sparkSessionInternal = TestSparkSession.createSparkSession
  }
}
