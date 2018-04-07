package otus.project.configuration

import org.apache.spark.sql.SparkSession

object MonitoringSettings {

  def getTagsToWatch(sparkSession: SparkSession): Seq[String] = {
    import sparkSession.sqlContext.implicits._
    sparkSession.sql("SELECT * FROM settings")
      .as[String]
      .distinct()
      .collect()
  }
}
