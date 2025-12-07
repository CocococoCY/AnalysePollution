import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object PicsHoraires {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    println("\n" + "=" * 80)
    println("Analyse des pics horaires et des périodes critiques")
    println("=" * 80)

    val spark = SparkSession.builder()
      .appName("Pics horaires et périodes critiques")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/pollution_big.csv")


    //Calcul indicateur de pollution
    val dfWithPollutionIndice = df.withColumn(
      "pollution_index",
        ((col("pm25") / 25) * 3 + (col("pm10") / 50) * 2 + (col("co2") / 1000) * 0.5 + (col("no2") / 40) * 2 + (col("O3") / 100) * 1) / 8.5 )

    println("=" * 80)
    println("pollution moyenne par heure de la journée")
    println("=" * 80)

    val pollutionParHeure = dfWithPollutionIndice
      .groupBy("hour")
      .agg(
        round(avg("pollution_index"), 2).alias("pollution_moyenne"),
        round(avg("pm25"), 2).alias("pm25_moyen"),
        round(avg("pm10"), 2).alias("pm10_moyen"),
        round(avg("co2"), 2).alias("co2_moyen"),
        round(avg("no2"), 2).alias("no2_moyen"),
        round(avg("noise"), 2).alias("bruit_moyen"),
        count("*").alias("nb_mesures")
      )
      .orderBy("hour")

    println("\nPollution moyenne par heure :\n")
    pollutionParHeure.show(24, truncate = false)

    println("=" * 80)
    println("heures où l'on observe les pics de pollution")
    println("=" * 80)

    val heuresPics = pollutionParHeure
      .orderBy(desc("pollution_moyenne"))
      .limit(5)

    println("\nles 5 heures les plus polluees :\n")
    heuresPics.show(truncate = false)

    println("=" * 80)
    println("heures où l'on observe une pollution basse")
    println("=" * 80)

    val heuresCalmes = pollutionParHeure
      .orderBy(asc("pollution_moyenne"))
      .limit(5)

    println("\nles 5 heures les moins polluees :\n")
    heuresCalmes.show(truncate = false)

    println("=" * 80)
    println("pollution par jours")
    println("=" * 80)

    val pollutionParJour = dfWithPollutionIndice
      .groupBy("day_of_week")
      .agg(
        round(avg("pollution_index"), 2).alias("pollution_moyenne"),
        round(avg("pm25"), 2).alias("pm25_moyen"),
        round(avg("pm10"), 2).alias("pm10_moyen"),
        round(avg("co2"), 2).alias("co2_moyen"),
        round(avg("no2"), 2).alias("no2_moyen"),
        round(avg("noise"), 2).alias("bruit_moyen"),
        count("*").alias("nb_mesures")
      )
      .withColumn("jour",
        when(col("day_of_week") === 1, "Lundi")
          .when(col("day_of_week") === 2, "Mardi")
          .when(col("day_of_week") === 3, "Mercredi")
          .when(col("day_of_week") === 4, "Jeudi")
          .when(col("day_of_week") === 5, "Vendredi")
          .when(col("day_of_week") === 6, "Samedi")
          .when(col("day_of_week") === 7, "Dimanche")
          .otherwise("Inconnu")
      )
      .select("jour", "pollution_moyenne", "pm25_moyen", "pm10_moyen", "co2_moyen", "bruit_moyen", "nb_mesures")
      .orderBy(desc("pollution_moyenne"))

    println("\nPollution par jour de la semaine :\n")
    pollutionParJour.show(truncate = false)

    println("=" * 80)
    println("5. Périodes critiques (heures de haute pollution lors des jours de haute pollution")
    println("=" * 80)

    val periodesCritiques = dfWithPollutionIndice
      .groupBy("day_of_week", "hour")
      .agg(
        round(avg("pollution_index"), 2).alias("pollution_moyenne"),
        count("*").alias("nb_mesures")
      )
      .withColumn("jour",
        when(col("day_of_week") === 1, "Lundi")
          .when(col("day_of_week") === 2, "Mardi")
          .when(col("day_of_week") === 3, "Mercredi")
          .when(col("day_of_week") === 4, "Jeudi")
          .when(col("day_of_week") === 5, "Vendredi")
          .when(col("day_of_week") === 6, "Samedi")
          .when(col("day_of_week") === 7, "Dimanche")
      )
      .withColumn("periode", concat(col("jour"), lit(" - "), col("hour"), lit("h")))
      .select("periode", "pollution_moyenne", "nb_mesures")
      .orderBy(desc("pollution_moyenne"))
      .limit(20)

    println("\nles 20 periodes les plus polluees :\n")
    periodesCritiques.show(20, truncate = false)

    spark.stop()
  }


}