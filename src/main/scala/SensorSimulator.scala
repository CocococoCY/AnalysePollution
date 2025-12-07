import java.io.{File, PrintWriter}
import scala.util.Random

object SensorSimulator extends App {
  val outputDir = "data/"
  val rand = new Random()

  while (true) {
    val stationId = rand.nextInt(50)
    val pm25 = rand.nextInt(150)
    val pm10 = rand.nextInt(150)
    val co2 = 500 + rand.nextInt(600)
    val no2 = rand.nextInt(150)
    val o3 = rand.nextInt(50)
    val timestamp = System.currentTimeMillis()

    val fileName = s"${outputDir}sensor_${timestamp}.csv"
    val pw = new PrintWriter(new File(fileName))
    pw.println(s"$stationId,$pm25,$pm10,$co2,$no2,$o3,$timestamp")
    pw.close()

    println(s"Nouvelle donnée captée  → $fileName")
    Thread.sleep(40000)
  }
}
