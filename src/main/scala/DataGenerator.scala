import org.apache.spark.sql._
import scala.util.Random
import java.io.{PrintWriter, File}

object DataGenerator {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    println("\n" + "=" * 80)
    println("Génération du jeu de données - Réseau de transport cohérent")
    println("=" * 80)

    val spark = SparkSession.builder()
      .appName("Generate Pollution Dataset - Transport")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Coordonnées des villes
    val citiesWithCoords = Map(
      "Paris" -> (48.8566, 2.3522),
      "Lyon" -> (45.7640, 4.8357),
      "Marseille" -> (43.2965, 5.3698),
      "Nice" -> (43.7102, 7.2620),
      "Toulouse" -> (43.6047, 1.4442)
    )

    // Structure cohérente : ville -> lignes -> stations (réduit pour lisibilité)
    val networkStructure = Map(
      "Paris" -> Map(
        "Metro 1" -> 8,
        "Metro 4" -> 7,
        "Metro 7" -> 9,
        "RER A" -> 10,
        "RER B" -> 8
      ),
      "Lyon" -> Map(
        "Metro A" -> 6,
        "Metro B" -> 7,
        "Metro D" -> 5
      ),
      "Marseille" -> Map(
        "Metro 1" -> 6,
        "Metro 2" -> 5
      ),
      "Nice" -> Map(
        "Tram 1" -> 7,
        "Tram 2" -> 6
      ),
      "Toulouse" -> Map(
        "Metro A" -> 8,
        "Metro B" -> 6
      )
    )

    println("\nStructure du réseau :")
    networkStructure.foreach { case (city, lines) =>
      println(s"  $city : ${lines.size} lignes, ${lines.values.sum} stations")
    }

    // Générer les stations de manière cohérente
    case class StationInfo(
                            stationId: Int,
                            stationName: String,
                            city: String,
                            line: String,
                            latitude: Double,
                            longitude: Double,
                            positionOnLine: Int,
                            pollutionFactor: Double  // Facteur multiplicatif de pollution (0.3 à 2.0)
                          )

    var stationIdCounter = 0
    val stations = networkStructure.flatMap { case (city, lines) =>
      val (baseLat, baseLon) = citiesWithCoords(city)

      lines.flatMap { case (lineName, nbStations) =>
        val rand = new Random(city.hashCode + lineName.hashCode)

        // Créer un tracé de ligne (direction aléatoire mais cohérente)
        val angle = rand.nextDouble() * 2 * Math.PI
        val spread = 0.15 // Étalement de la ligne

        (0 until nbStations).map { position =>
          val progress = position.toDouble / (nbStations - 1)

          // Position le long de la ligne avec un peu de variation
          val lat = baseLat + Math.cos(angle) * spread * progress + (rand.nextDouble() - 0.5) * 0.02
          val lon = baseLon + Math.sin(angle) * spread * progress + (rand.nextDouble() - 0.5) * 0.02

          // NOUVEAU : Facteur de pollution variable par station (0.3 à 2.0)
          // Certaines stations sont naturellement plus polluées (proximité routes, trafic élevé, etc.)
          // D'autres sont dans des zones calmes (espaces verts, zones résidentielles)
          val pollutionFactor = 0.3 + rand.nextDouble() * 1.7

          val stationInfo = StationInfo(
            stationId = stationIdCounter,
            stationName = s"$city-$lineName-S$position",
            city = city,
            line = lineName,
            latitude = lat,
            longitude = lon,
            positionOnLine = position,
            pollutionFactor = pollutionFactor  // Ajout du facteur
          )

          stationIdCounter += 1
          stationInfo
        }
      }
    }.toSeq

    println(s"\nTotal de stations créées : ${stations.size}")

    // Générer des mesures temporelles pour chaque station
    val nbMeasurementsPerStation = 1000  // Augmenté pour atteindre ~100k lignes
    val nbRows = stations.size * nbMeasurementsPerStation

    println(s"Génération de $nbRows mesures ($nbMeasurementsPerStation par station)...\n")

    val data = stations.flatMap { station =>
      (0 until nbMeasurementsPerStation).map { measurementId =>
        val rand = new Random(station.stationId * 1000 + measurementId + 42)

        val timestamp = System.currentTimeMillis() - rand.nextInt(365 * 24) * 3600 * 1000L
        val hour = rand.nextInt(24)
        val dayOfWeek = rand.nextInt(7) + 1
        val month = rand.nextInt(12) + 1

        val humidity = 40 + rand.nextInt(60)
        val temperature = 5 + rand.nextInt(30)
        val wind = rand.nextDouble() * 10
        val rain = if (humidity > 70 && rand.nextDouble() > 0.6) 1 else 0

        val isTransportStopped = hour >= 1 && hour < 5
        val isRushHour = (hour >= 7 && hour <= 9) || (hour >= 17 && hour <= 20)

        // Valeurs de base
        val basePm25 = if (isTransportStopped) 0
        else if (isRushHour) 40 + rand.nextInt(40)
        else 15 + rand.nextInt(30)

        val basePm10 = if (isTransportStopped) 0
        else if (isRushHour) 60 + rand.nextInt(40)
        else 25 + rand.nextInt(35)

        val baseCo2 = if (isTransportStopped) 0
        else if (isRushHour) 700 + rand.nextInt(300)
        else 450 + rand.nextInt(250)

        val baseNo2 = if (isTransportStopped) 0
        else if (isRushHour) 30 + rand.nextInt(50)
        else 10 + rand.nextInt(30)

        // Appliquer le facteur de pollution de la station
        val pm25 = (basePm25 * station.pollutionFactor).toInt
        val pm10 = (basePm10 * station.pollutionFactor).toInt
        val co2 = (baseCo2 * station.pollutionFactor).toInt
        val no2 = (baseNo2 * station.pollutionFactor).toInt

        val o3 = if (month >= 5 && month <= 9 && hour >= 12 && hour <= 16) 25 + rand.nextInt(35)
        else 5 + rand.nextInt(20)

        val noise = if (isTransportStopped) 30 + rand.nextInt(10)
        else if (isRushHour) 70 + rand.nextInt(15)
        else if (hour >= 5 && hour <= 23) 55 + rand.nextInt(15)
        else 35 + rand.nextInt(15)

        (station.stationId, station.stationName, station.city, station.latitude, station.longitude, station.line,
          timestamp, hour, dayOfWeek, month,
          pm25, pm10, co2, no2, o3, noise,
          humidity, temperature, wind, rain)
      }
    }

    try {
      // Créer dossier data
      val dataDir = new File("data")
      if (!dataDir.exists()) {
        dataDir.mkdirs()
        println("Dossier data/ créé")
      }

      // Supprimer ancien csv
      val csvFile = new File("data/pollution_big.csv")
      if (csvFile.exists()) {
        csvFile.delete()
        println("Ancien fichier supprimé")
      }

      // Nouveau fichier
      val writer = new PrintWriter(csvFile)

      // En-tête du tableau
      writer.println("station_id,station_name,city,latitude,longitude,line,timestamp,hour,day_of_week,month,pm25,pm10,co2,no2,o3,noise,humidity,temperature,wind_speed,rain")

      var duplicateCount = 0
      var missingCount = 0
      var lastLine = ""

      // Données avec défauts
      data.zipWithIndex.foreach { case (row, idx) =>
        val (stationId, stationName, city, lat, lon, line, ts, h, dow, m, pm25, pm10, co2, no2, o3, noise, hum, temp, wind, rain) = row
        val rand = new Random(idx + 12345)

        // Doublons (1% de chance)
        val isDuplicate = rand.nextDouble() < 0.01
        if (isDuplicate && idx > 0) {
          writer.println(lastLine)
          duplicateCount += 1
        } else {
          // Valeurs manquantes (2% de chance par colonne sensible)
          val pm25Str = if (rand.nextDouble() < 0.02) { missingCount += 1; "" } else pm25.toString
          val pm10Str = if (rand.nextDouble() < 0.02) { missingCount += 1; "" } else pm10.toString
          val co2Str = if (rand.nextDouble() < 0.02) { missingCount += 1; "" } else co2.toString
          val no2Str = if (rand.nextDouble() < 0.02) { missingCount += 1; "" } else no2.toString
          val tempStr = if (rand.nextDouble() < 0.02) { missingCount += 1; "" } else temp.toString
          val humStr = if (rand.nextDouble() < 0.02) { missingCount += 1; "" } else hum.toString
          val windStr = if (rand.nextDouble() < 0.02) { missingCount += 1; "" } else wind.toString

          val currentLine = s"$stationId,$stationName,$city,$lat,$lon,$line,$ts,$h,$dow,$m,$pm25Str,$pm10Str,$co2Str,$no2Str,$o3,$noise,$humStr,$tempStr,$windStr,$rain"
          writer.println(currentLine)
          lastLine = currentLine
        }
      }

      writer.close()

      println("=" * 80)
      println(" Succès ")
      println("=" * 80)
      println(s"\n✓ Fichier généré : data/pollution_big.csv")
      println(s"✓ Nombre total de lignes : ${data.size}")
      println(s"✓ Doublons introduits : $duplicateCount")
      println(s"✓ Valeurs manquantes introduites : $missingCount")
      println("\n" + "=" * 80 + "\n")

    } catch {
      case e: Exception =>
        println("Erreur lors de l'écriture du fichier ")
        e.printStackTrace()
    }

    spark.stop()
  }
}