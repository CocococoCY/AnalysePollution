import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DetectionAnomalies {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    println("\n" + "=" * 80)
    println("Detection automatique des anomalies")
    println("=" * 80)

    val spark = SparkSession.builder()
      .appName("Detection Anomalies")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/pollution_big.csv")

    println(s"${df.count()} lignes chargees\n")

    // ========================================
    // Méthode 1: détection des anomalies par seuils fixes (normes OMS)
    // ========================================
    println("=" * 80)
    println("Méthode 1: détection des anomalies par seuils fixes (normes OMS)")
    println("=" * 80)

    // Seuils critiques OMS
    val seuilCritiquePM25 = 75.0    // 3x le seuil OMS
    val seuilCritiquePM10 = 150.0   // 3x le seuil OMS
    val seuilCritiqueNO2 = 100.0    // 2.5x le seuil OMS
    val seuilCritiqueO3 = 200.0     // 2x le seuil OMS
    val seuilCritiqueCO2 = 1500.0   // 1.5x le seuil
    val tempMin = 0.0
    val tempMax = 45.0

    val dfAnomaliesSeuils = df.withColumn("anomalie_type",
      when(col("pm25") > seuilCritiquePM25, "PM2.5_CRITIQUE")
        .when(col("pm10") > seuilCritiquePM10, "PM10_CRITIQUE")
        .when(col("no2") > seuilCritiqueNO2, "NO2_CRITIQUE")
        .when(col("o3") > seuilCritiqueO3, "O3_CRITIQUE")
        .when(col("co2") > seuilCritiqueCO2, "CO2_CRITIQUE")
        .when(col("temperature") < tempMin || col("temperature") > tempMax, "TEMP_IMPOSSIBLE")
        .otherwise("NORMAL")
    )

    val anomaliesSeuils = dfAnomaliesSeuils
      .filter(col("anomalie_type") =!= "NORMAL")

    val nbAnomaliesSeuils = anomaliesSeuils.count()

    println(s"\nNombre d'anomalies detectees : $nbAnomaliesSeuils")
    println("\nExemples d'anomalies detectees (seuils fixes) :\n")

    anomaliesSeuils
      .select("station_name", "hour", "day_of_week", "anomalie_type", "pm25", "pm10", "no2", "o3", "co2", "temperature")
      .orderBy(desc("pm25"))
      .show(10, truncate = false)

    println("\nRepartition des anomalies par type :\n")
    anomaliesSeuils
      .groupBy("anomalie_type")
      .count()
      .orderBy(desc("count"))
      .show(truncate = false)

    // ========================================
    // Méthode 2: anomalies par z-score
    // ========================================
    println("=" * 80)
    println("Méthode 2: anomalies par z-score ")
    println("=" * 80)

    // Calculer moyenne et ecart-type pour chaque polluant
    val stats = df.select(
      avg("pm25").alias("mean_pm25"),
      stddev("pm25").alias("std_pm25"),
      avg("pm10").alias("mean_pm10"),
      stddev("pm10").alias("std_pm10"),
      avg("co2").alias("mean_co2"),
      stddev("co2").alias("std_co2"),
      avg("no2").alias("mean_no2"),
      stddev("no2").alias("std_no2")
    ).first()

    val meanPM25 = stats.getDouble(0)
    val stdPM25 = stats.getDouble(1)
    val meanPM10 = stats.getDouble(2)
    val stdPM10 = stats.getDouble(3)
    val meanCO2 = stats.getDouble(4)
    val stdCO2 = stats.getDouble(5)
    val meanNO2 = stats.getDouble(6)
    val stdNO2 = stats.getDouble(7)

    println(s"\nStatistiques globales :")
    println(f"PM2.5 : moyenne = $meanPM25%.2f, ecart-type = $stdPM25%.2f")
    println(f"PM10  : moyenne = $meanPM10%.2f, ecart-type = $stdPM10%.2f")
    println(f"CO2   : moyenne = $meanCO2%.2f, ecart-type = $stdCO2%.2f")
    println(f"NO2   : moyenne = $meanNO2%.2f, ecart-type = $stdNO2%.2f")

    // Calculer Z-scores
    val dfWithZScores = df.withColumn("z_pm25", abs((col("pm25") - meanPM25) / stdPM25))
      .withColumn("z_pm10", abs((col("pm10") - meanPM10) / stdPM10))
      .withColumn("z_co2", abs((col("co2") - meanCO2) / stdCO2))
      .withColumn("z_no2", abs((col("no2") - meanNO2) / stdNO2))

    // Detecter anomalies (|z| > 3)
    val anomaliesZScore = dfWithZScores.filter(
      col("z_pm25") > 3 || col("z_pm10") > 3 || col("z_co2") > 3 || col("z_no2") > 3
    ).withColumn("anomalie_zscore",
      when(col("z_pm25") > 3, "PM2.5_OUTLIER")
        .when(col("z_pm10") > 3, "PM10_OUTLIER")
        .when(col("z_co2") > 3, "CO2_OUTLIER")
        .when(col("z_no2") > 3, "NO2_OUTLIER")
        .otherwise("NORMAL")
    )

    val nbAnomaliesZScore = anomaliesZScore.count()

    println(s"\n\nNombre d'anomalies detectees (Z-score) : $nbAnomaliesZScore")
    println("\nExemples d'anomalies Z-score (|z| > 3) :\n")

    anomaliesZScore
      .select("station_name", "hour", "anomalie_zscore", "pm25", "z_pm25", "pm10", "z_pm10", "co2", "z_co2")
      .orderBy(desc("z_pm25"))
      .show(10, truncate = false)

    // ========================================
    // Méthode 3: Anomalie par méthode des quartiles
    // ========================================
    println("=" * 80)
    println("Méthode 3: Anomalie par méthode des quartiles")
    println("=" * 80)

    // Calculer quartiles pour PM2.5
    val quantiles = df.stat.approxQuantile("pm25", Array(0.25, 0.75), 0.01)
    val q1_pm25 = quantiles(0)
    val q3_pm25 = quantiles(1)
    val iqr_pm25 = q3_pm25 - q1_pm25
    val lowerBound_pm25 = q1_pm25 - 1.5 * iqr_pm25
    val upperBound_pm25 = q3_pm25 + 1.5 * iqr_pm25

    println(s"\nStatistiques IQR pour PM2.5 :")
    println(f"Q1 (25e percentile) = $q1_pm25%.2f")
    println(f"Q3 (75e percentile) = $q3_pm25%.2f")
    println(f"IQR = $iqr_pm25%.2f")
    println(f"Borne inferieure = $lowerBound_pm25%.2f")
    println(f"Borne superieure = $upperBound_pm25%.2f")

    val anomaliesIQR = df.filter(
      col("pm25") < lowerBound_pm25 || col("pm25") > upperBound_pm25
    )

    val nbAnomaliesIQR = anomaliesIQR.count()

    println(s"\n\nNombre d'anomalies detectees (IQR) : $nbAnomaliesIQR")
    println("\nExemples d'anomalies IQR (hors des bornes) :\n")

    anomaliesIQR
      .select("station_name", "hour", "day_of_week", "pm25", "pm10", "co2", "no2")
      .orderBy(desc("pm25"))
      .show(10, truncate = false)

    // ========================================
    // Méthode 4: Anomalies de logique
    // ========================================
    println("=" * 80)
    println("Méthode 4: Anomalies de logique")
    println("=" * 80)

    // Detecter incoherences logiques
    val anomaliesContextuelles = df.filter(
      // PM2.5 = 0 pendant heures de pointe (anormal)
      (col("hour").between(7, 9) || col("hour").between(17, 20)) && col("pm25") === 0 ||
        // Pollution elevee la nuit (1h-5h) - transport arrete
        col("hour").between(1, 5) && col("pm25") > 30 ||
        // Temperature impossible
        col("temperature") < -10 || col("temperature") > 50
    ).withColumn("anomalie_contexte",
      when((col("hour").between(7, 9) || col("hour").between(17, 20)) && col("pm25") === 0,
        "POLLUTION_NULLE_POINTE")
        .when(col("hour").between(1, 5) && col("pm25") > 30,
          "POLLUTION_ELEVEE_NUIT")
        .when(col("temperature") < -10 || col("temperature") > 50,
          "TEMP_EXTREME")
        .otherwise("AUTRE")
    )

    val nbAnomaliesContextuelles = anomaliesContextuelles.count()

    println(s"\nNombre d'anomalies contextuelles detectees : $nbAnomaliesContextuelles")

    if (nbAnomaliesContextuelles > 0) {
      println("\nExemples d'anomalies contextuelles :\n")
      anomaliesContextuelles
        .select("station_name", "hour", "day_of_week", "anomalie_contexte", "pm25", "pm10", "co2", "temperature")
        .show(10, truncate = false)

      println("\nRepartition des anomalies contextuelles :\n")
      anomaliesContextuelles
        .groupBy("anomalie_contexte")
        .count()
        .orderBy(desc("count"))
        .show(truncate = false)
    } else {
      println("\nAucune anomalie contextuelle detectee (donnees coherentes)")
    }

    // ========================================
    // Résumé
    // ========================================
    println("\n" + "=" * 80)
    println("Résomé des anomalies détectées")
    println("=" * 80)

    val totalLignes = df.count()
    val pctSeuils = (nbAnomaliesSeuils.toDouble / totalLignes) * 100
    val pctZScore = (nbAnomaliesZScore.toDouble / totalLignes) * 100
    val pctIQR = (nbAnomaliesIQR.toDouble / totalLignes) * 100
    val pctContexte = (nbAnomaliesContextuelles.toDouble / totalLignes) * 100

    println(f"\nTotal de lignes : $totalLignes")
    println(f"\n1. Anomalies par seuils fixes : $nbAnomaliesSeuils ($pctSeuils%.2f%%)")
    println(f"2. Anomalies par Z-score      : $nbAnomaliesZScore ($pctZScore%.2f%%)")
    println(f"3. Anomalies par IQR          : $nbAnomaliesIQR ($pctIQR%.2f%%)")
    println(f"4. Anomalies contextuelles    : $nbAnomaliesContextuelles ($pctContexte%.2f%%)")

    println("\n" + "=" * 80)
    println("Fin d'analyse")
    println("=" * 80 + "\n")

    spark.stop()
  }
}