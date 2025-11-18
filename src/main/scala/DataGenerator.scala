import org.apache.spark.sql._
import scala.util.Random
import java.io.{PrintWriter, File}

object DataGenerator {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Generate Pollution Dataset - Transport")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val citiesWithCoords = Map(
      "Paris" -> (48.8566, 2.3522),
      "Lyon" -> (45.7640, 4.8357),
      "Marseille" -> (43.2965, 5.3698),
      "Nice" -> (43.7102, 7.2620),
      "Toulouse" -> (43.6047, 1.4442)
    )

    val cities = citiesWithCoords.keys.toSeq
    val lines = Seq("Ligne A", "Ligne B", "Ligne C", "Metro 1", "Metro 2")
    val nbRows = 100000

    // Générer les données
    val data = (0 until nbRows).map { id =>
      val rand = new Random(id + 42)

      val city = cities(rand.nextInt(cities.length))
      val stationId = rand.nextInt(50)
      val stationName = s"Station-$stationId"
      val line = lines(rand.nextInt(lines.length))

      val (baseLat, baseLon) = citiesWithCoords(city)
      val latitude = baseLat + (rand.nextDouble() - 0.5) * 0.2
      val longitude = baseLon + (rand.nextDouble() - 0.5) * 0.2

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

      val pm25 = if (isTransportStopped) 0
      else if (isRushHour) 40 + rand.nextInt(40)
      else 15 + rand.nextInt(30)

      val pm10 = if (isTransportStopped) 0
      else if (isRushHour) 60 + rand.nextInt(40)
      else 25 + rand.nextInt(35)

      val co2 = if (isTransportStopped) 0
      else if (isRushHour) 700 + rand.nextInt(300)
      else 450 + rand.nextInt(250)

      val no2 = if (isTransportStopped) 0
      else if (isRushHour) 30 + rand.nextInt(50)
      else 10 + rand.nextInt(30)

      val o3 = if (month >= 5 && month <= 9 && hour >= 12 && hour <= 16) 25 + rand.nextInt(35)
      else 5 + rand.nextInt(20)

      val noise = if (isTransportStopped) 30 + rand.nextInt(10)
      else if (isRushHour) 70 + rand.nextInt(15)
      else if (hour >= 5 && hour <= 23) 55 + rand.nextInt(15)
      else 35 + rand.nextInt(15)

      (stationId, stationName, city, latitude, longitude, line,
        timestamp, hour, dayOfWeek, month,
        pm25, pm10, co2, no2, o3, noise,
        humidity, temperature, wind, rain)
    }

    try {
      //dossier data
      val dataDir = new File("data")
      if (!dataDir.exists()) {
        dataDir.mkdirs()
      }

      // supprimé ancien csv
      val csvFile = new File("data/pollution_big.csv")
      if (csvFile.exists()) {
        csvFile.delete()
      }

      // nouveau fichier
      val writer = new PrintWriter(csvFile)

      // en-tête du tableau
      writer.println("station_id,station_name,city,latitude,longitude,line,timestamp,hour,day_of_week,month,pm25,pm10,co2,no2,o3,noise,humidity,temperature,wind_speed,rain")

      var duplicateCount = 0
      var missingCount = 0
      var lastLine = ""

      // données avec doublons
      data.zipWithIndex.foreach { case (row, idx) =>
        val (stationId, stationName, city, lat, lon, line, ts, h, dow, m, pm25, pm10, co2, no2, o3, noise, hum, temp, wind, rain) = row
        val rand = new Random(idx + 12345) // Seed différent pour les défauts

        // doublons (1% de chance)
        val isDuplicate = rand.nextDouble() < 0.01
        if (isDuplicate && idx > 0) {
          // on réécrit la ligne précédente
          writer.println(lastLine)
          duplicateCount += 1
        } else {
          // valeurs manquantes (2% de chance par colonne sensible)
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

      println("jeu de données créé avec succès")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    spark.stop()
  }
}