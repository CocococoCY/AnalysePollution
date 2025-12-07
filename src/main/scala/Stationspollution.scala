import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

object Stationspollution {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    println("\n" + "=" * 80)
    println("Détection des stations les plus polluées")
    println("=" * 80)

    val spark = SparkSession.builder()
      .appName("Stations polluees")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/pollution_big.csv")

    println(s"${df.count()} lignes chargees\n")

    val rdd = df.rdd

    // ========================================
    // SEUILS OMS (constantes)
    // ========================================
    val seuilPM25 = 25.0
    val seuilPM10 = 50.0
    val seuilNO2 = 40.0
    val seuilO3 = 100.0
    val seuilCO2 = 1000.0

    // ========================================
    // Calcul de l'indice de pollution, basé sur seuils OMS (fonction pure)
    // ========================================
    def calculerIndicePollution(pm25: Int, pm10: Int, co2: Int, no2: Int, o3: Int): Double = {
      (
        (pm25.toDouble / seuilPM25) * 3 +
          (pm10.toDouble / seuilPM10) * 2 +
          (co2.toDouble / seuilCO2) * 0.5 +
          (no2.toDouble / seuilNO2) * 2 +
          (o3.toDouble / seuilO3) * 1
        ) / 8.5
    }

    // ========================================
    // Extraction et calcul de l'indice
    // ========================================
    println("=" * 80)
    println("Calcul de l'indice de pollution pour chaque mesure")
    println("=" * 80)

    val stationInfoIndiceRDD: RDD[(Int, String, String, String, Double)] = rdd.map { row =>
      val stationId = row.getAs[Int]("station_id")
      val stationName = row.getAs[String]("station_name")
      val city = row.getAs[String]("city")
      val line = row.getAs[String]("line")
      val pm25 = row.getAs[Int]("pm25")
      val pm10 = row.getAs[Int]("pm10")
      val co2 = row.getAs[Int]("co2")
      val no2 = row.getAs[Int]("no2")
      val o3 = row.getAs[Int]("o3")

      val indice = calculerIndicePollution(pm25, pm10, co2, no2, o3)

      (stationId, stationName, city, line, indice)
    }

    println("\nExemples de transformations (station_id, station_name, city, line, indice) :")
    stationInfoIndiceRDD.take(3).foreach { case (id, name, city, line, indice) =>
      println(f"  Station $id - $name ($city, $line) : $indice%.2f")
    }

    // ========================================
    // Preparation pour agregation
    // ========================================
    println("\n" + "=" * 80)
    println("Preparation de la cle composee (station_id, station_name, city, line)")
    println("=" * 80)

    val stationCleIndiceRDD = stationInfoIndiceRDD.map { case (id, name, city, line, indice) =>
      ((id, name, city, line), indice)
    }

    println("\nStructure de donnees preparee pour l'agregation")

    // ========================================
    // Calcul statistiques completes
    // ========================================
    println("\n" + "=" * 80)
    println("Calcul des statistiques par station")
    println("=" * 80)

    type StatsAccum = (Double, Int, Double, Double)

    val stationStatsRDD: RDD[((Int, String, String, String), StatsAccum)] =
      stationCleIndiceRDD.aggregateByKey(
        (0.0, 0, Double.MaxValue, Double.MinValue)  //valeur de depart
      )(
        { case ((somme, count, min, max), indice) =>
          (somme + indice, count + 1, math.min(min, indice), math.max(max, indice))
        },
        { case ((s1, c1, min1, max1), (s2, c2, min2, max2)) =>
          (s1 + s2, c1 + c2, math.min(min1, min2), math.max(max1, max2))
        }
      )

    println("Statistiques agregees par station (somme, count, min, max)")

    // ========================================
    // Calcul de la moyenne
    // ========================================
    println("\n" + "=" * 80)
    println("Calcul de la moyenne a partir des statistiques")
    println("=" * 80)

    val stationMoyenneRDD = stationStatsRDD.mapValues { case (somme, count, min, max) =>
      val moyenne = somme / count
      (moyenne, min, max, count)
    }

    println("Moyenne calculee pour chaque station")

    // ========================================
    // Restructuration pour le tri
    // ========================================
    println("\n" + "=" * 80)
    println("Restructuration des donnees pour faciliter le tri")
    println("=" * 80)

    val stationForSortRDD = stationMoyenneRDD.map {
      case ((id, name, city, line), (moyenne, min, max, count)) =>
        (moyenne, (id, name, city, line, min, max, count))
    }

    println("Donnees restructurees avec moyenne comme cle pour le tri")

    // ========================================
    // Tri par pollution decroissante
    // ========================================
    println("\n" + "=" * 80)
    println("Tri par pollution decroissante")
    println("=" * 80)

    val stationsTrieesRDD = stationForSortRDD.sortByKey(ascending = false)

    println("Stations triees par ordre decroissant de pollution")

    // ========================================
    // Reformatage final
    // ========================================
    println("\n" + "=" * 80)
    println("Reformatage pour l'affichage final")
    println("=" * 80)

    val stationsFinalRDD = stationsTrieesRDD.map {
      case (moyenne, (id, name, city, line, min, max, count)) =>
        (id, name, city, line, moyenne, min, max, count)
    }

    println("Donnees reformatees pour affichage")

    // ========================================
    // Selection des 10 premieres
    // ========================================
    println("\n" + "=" * 80)
    println("Selection des 10 stations les plus polluées, en moyenne")
    println("=" * 80)

    val top10Stations = stationsFinalRDD.take(10)

    println("\nTop 10 des stations les plus polluées :\n")
    println(f"${"ID"}%4s ${"Station"}%-15s ${"Ville"}%-12s ${"Ligne"}%-10s ${"Moyenne"}%8s ${"Min"}%8s ${"Max"}%8s ${"Mesures"}%8s")
    println("-" * 90)

    top10Stations.foreach { case (id, name, city, line, moyenne, min, max, count) =>
      println(f"$id%4d $name%-15s $city%-12s $line%-10s $moyenne%8.2f $min%8.2f $max%8.2f $count%8d")
    }

    // ========================================
    // Stations avec pics extremes
    // ========================================
    println("\n" + "=" * 80)
    println("Selection des stations avec pics extremes (max > 2.0)")
    println("=" * 80)

    val stationsAvecPics = stationsFinalRDD.filter {
      case (_, _, _, _, _, _, max, _) => max > 2.0
    }

    val top10Pics = stationsAvecPics.take(10)

    println("\nLes 10 stations avec les pics de pollution les plus importants: :\n")
    println(f"${"ID"}%4s ${"Station"}%-15s ${"Ville"}%-12s ${"Ligne"}%-10s ${"Max"}%8s ${"Moyenne"}%8s")
    println("-" * 70)

    top10Pics.foreach { case (id, name, city, line, moyenne, _, max, _) =>
      println(f"$id%4d $name%-15s $city%-12s $line%-10s $max%8.2f $moyenne%8.2f")
    }

    // ========================================
    // Pollution totale du reseau
    // ========================================
    println("\n" + "=" * 80)
    println("Calcul de la pollution totale du reseau")
    println("=" * 80)

    val indicesRDD = stationCleIndiceRDD.map { case (_, indice) => indice }

    val pollutionTotale = indicesRDD.reduce(_ + _)
    val nbMesures = indicesRDD.count()
    val moyenneGlobale = pollutionTotale / nbMesures

    println(f"\nPollution totale du reseau : $pollutionTotale%.2f")
    println(f"Nombre total de mesures : $nbMesures")
    println(f"Moyenne globale : $moyenneGlobale%.2f")

    // ========================================
    // Calcul avec valeur initiale (usage de fold)
    // ========================================
    println("\n" + "=" * 80)
    println("Calcul avec valeur initiale (usage de fold)")
    println("=" * 80)

    val pollutionTotaleFold = indicesRDD.fold(0.0)(_ + _)

    println(f"Pollution totale (avec fold) : $pollutionTotaleFold%.2f")

    println("=" * 80)
    println("Fin d'analyse")
    println("=" * 80 + "\n")

    spark.stop()
  }
}