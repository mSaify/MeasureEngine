package measureenginelab

import java.io.Closeable

import org.apache.spark.sql.SparkSession

object Helpers {
  def createSession(): SparkSession =
    SparkSession
      .builder()
      .appName("My Test App - LOS")
      .enableHiveSupport()
      .getOrCreate()

  // Automatic resource cleanup
  def using[A, B](resource: A)(f: A => B): B = {
    try {
      f(resource)
    }
    finally {
      resource match {
        case c : Closeable => c.close()
      }
    }
  }
}
