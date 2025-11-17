import org.apache.spark.sql.SparkSession
case class PollutionMeasurement(
                                 station_id: Int,
                                 station_name: String,
                                 city: String,
                                 latitude: Double,
                                 longitude: Double,
                                 line: String,
                                 timestamp: Long,
                                 hour: Int,
                                 day_of_week: Int,
                                 month: Int,
                                 pm25: Int,
                                 pm10: Int,
                                 co2: Int,
                                 no2: Int,
                                 o3: Int,
                                 noise: Int,
                                 humidity: Int,
                                 temperature: Int,
                                 wind_speed: Double,
                                 rain: Int
                               )


object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("AnalysePollution")
      .master("local[*]")
      .getOrCreate()

    // 1. Lecture du dataset brut (généré par DataGenerator)
    val dfRaw = Loader.readCSV(spark, "data/pollution_big.csv")

    println(s"Nombre de lignes brutes : ${dfRaw.count()}")

    // 2. Nettoyage : doublons + valeurs manquantes
    val dfClean = Cleaner.clean(dfRaw)

    println(s"Nombre de lignes après nettoyage : ${dfClean.count()}")


    // 3. Petit check : afficher le schéma et un aperçu
    dfClean.printSchema()
    dfClean.show(10, truncate = false)

    // 4. Exemple d’analyse : moyenne des polluants par ville
    dfClean
      .groupBy("city")
      .avg("pm25", "pm10", "co2", "no2", "o3")
      .show()

    spark.stop()
  }
}
