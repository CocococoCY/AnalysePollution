import org.apache.spark.sql.{DataFrame, SparkSession}

object Loader {

  // Lecture générique d'un CSV avec en-tête et inférence de schéma
  def readCSV(spark: SparkSession, path: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
}

