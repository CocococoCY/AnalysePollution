import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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

    import spark.implicits._

    // ----------------------------------------------------------------------
    // 1. Lecture du dataset brut (CSV généré par DataGenerator)
    // ----------------------------------------------------------------------
    val dfRaw = Loader.readCSV(spark, "data/pollution_big.csv")

    println(s" Nombre de lignes brutes : ${dfRaw.count()}")

    // ----------------------------------------------------------------------
    // 2. Nettoyage — suppression des doublons + valeurs manquantes
    // ----------------------------------------------------------------------
    val dfClean = Cleaner.clean(dfRaw)

    println(s" Nombre de lignes après nettoyage : ${dfClean.count()}")

    // Preview
    dfClean.show(5, truncate = false)


    // ----------------------------------------------------------------------
    // 3. PARTIE RDD — map, filter, flatMap (programmation fonctionnelle)
    // ----------------------------------------------------------------------
    val rdd = dfClean.rdd

    // ----- map : création d'un indice de pollution -----
    val pollutionIndexRDD = rdd.map { row =>
      val pm25 = row.getAs[Int]("pm25")
      val pm10 = row.getAs[Int]("pm10")
      val co2  = row.getAs[Int]("co2")

      val index = pm25 * 0.5 + pm10 * 0.3 + co2 * 0.2

      (row.getAs[String]("station_name"), index)
    }

    println("\n Exemple map() — Indice pollution :")
    pollutionIndexRDD.take(5).foreach(println)


    // ----- filter : stations très polluées -----
    val highPollutionRDD = rdd.filter(row => row.getAs[Int]("pm25") > 70)

    println("\n Exemple filter() — PM2.5 > 70 :")
    highPollutionRDD.take(5).foreach(println)


    // ----- flatMap : découpage du nom de station -----
    val flatNamesRDD = rdd.flatMap { row =>
      row.getAs[String]("station_name").split("-")
    }

    println("\n Exemple flatMap() — Mots dans station_name :")
    flatNamesRDD.take(10).foreach(println)


    // ----------------------------------------------------------------------
    // 4. Statistiques par station & par ligne
    // ----------------------------------------------------------------------

    // ----- Moyennes par station -----
    val statsByStation = dfClean
      .groupBy("station_id", "station_name")
      .agg(
        avg("pm25").alias("pm25_avg"),
        max("pm25").alias("pm25_max"),
        min("pm25").alias("pm25_min")
      )

    println("\n Statistiques par station :")
    statsByStation.show(10, truncate = false)


    // ----- Statistiques par ligne -----
    val statsByLine = dfClean
      .groupBy("line")
      .agg(
        avg("co2").alias("co2_avg"),
        max("co2").alias("co2_max"),
        min("co2").alias("co2_min")
      )

    println("\n Statistiques par ligne :")
    statsByLine.show(10, truncate = false)


    // ----------------------------------------------------------------------
    // 5. Extraction temporelle
    // ----------------------------------------------------------------------

    val dfTime = dfClean
      .withColumn("datetime", from_unixtime(col("timestamp") / 1000))
      .withColumn("year", year(col("datetime")))
      .withColumn("month_extracted", month(col("datetime")))
      .withColumn("day_extracted", dayofmonth(col("datetime")))
      .withColumn("hour_extracted", hour(col("datetime")))

    println("\n Extraction temporelle :")
    dfTime.select("timestamp", "datetime", "year", "month_extracted", "day_extracted", "hour_extracted")
      .show(10, truncate = false)


    // ----------------------------------------------------------------------
    spark.stop()
  }
}
