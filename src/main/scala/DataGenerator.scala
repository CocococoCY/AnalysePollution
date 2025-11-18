import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Random

object DataGenerator {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Generate Pollution Dataset")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val cities = Seq("Paris", "Lyon", "Marseille", "Nice", "Toulouse")
    val lines = Seq("Ligne A", "Ligne B", "Ligne C", "Metro 1", "Metro 2")

    val nbRows = 100000

    val df = spark.range(nbRows)
      .map { id =>
        val rand = new Random()

        val city = cities(rand.nextInt(cities.length))
        val stationId = rand.nextInt(50)
        val stationName = s"Station-$stationId"
        val line = lines(rand.nextInt(lines.length))

        val latitude = 48.0 + rand.nextDouble()
        val longitude = 2.0 + rand.nextDouble()

        val timestamp = System.currentTimeMillis() - rand.nextInt(365 * 24) * 3600 * 1000L
        val hour = rand.nextInt(24)
        val dayOfWeek = rand.nextInt(7) + 1
        val month = rand.nextInt(12) + 1

        val pm25 = 20 + rand.nextInt(60)
        val pm10 = 30 + rand.nextInt(70)
        val co2 = 400 + rand.nextInt(600)
        val no2 = 10 + rand.nextInt(80)
        val o3 = 5 + rand.nextInt(60)
        val noise = 50 + rand.nextInt(30)

        val humidity = 40 + rand.nextInt(60)
        val temperature = 5 + rand.nextInt(30)
        val wind = rand.nextDouble() * 10
        val rain = rand.nextInt(2)

        (stationId, stationName, city, latitude, longitude, line,
          timestamp, hour, dayOfWeek, month,
          pm25, pm10, co2, no2, o3, noise,
          humidity, temperature, wind, rain)
      }.toDF(
        "station_id", "station_name", "city", "latitude", "longitude", "line",
        "timestamp", "hour", "day_of_week", "month",
        "pm25", "pm10", "co2", "no2", "o3", "noise",
        "humidity", "temperature", "wind_speed", "rain"
      )

    df.write
      .option("header", "true")
      .mode("overwrite")
      .csv("data/pollution_big.csv")

    println("Dataset généré avec succès ✔")
  }
}
