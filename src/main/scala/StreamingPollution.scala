import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object StreamingPollution {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("StreamingPollution")
      .master("local[*]")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    import spark.implicits._

    spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")

    println("Lecture démarrée : lecture du dossier data/ ...")

    // Lecture en streaming des CSV créés par SensorSimulator
    val dfStreaming = spark.readStream
      .format("csv")
      .option("header", "false")
      .schema("station_id INT, pm25 INT, pm10 INT, co2 INT, no2 INT, o3 INT, timestamp LONG")
      .load("data/")

    val seuilPM25 = 25.0
    val seuilPM10 = 50.0
    val seuilNO2 = 40.0
    val seuilO3 = 100.0
    val seuilCO2 = 1000.0

    //Transformation : On calcul un indice globale
    val transformed = dfStreaming
      .withColumn("pollution_index",
        ($"pm25"/seuilPM25 * 3 + $"pm10"/seuilPM10 * 2 + $"co2"/seuilCO2 * 0.5 + $"no2"/seuilNO2 * 2 + $"o3"/seuilO3 * 1)/8.5
      )
      .withColumn("severity",
        when($"pollution_index" > 1, "DANGER")
          .when($"pollution_index" > 0.5, "ELEVEE")
          .otherwise("NORMALE")
      )

    // Affichage  mesures anormales
    val query = transformed
      .filter($"severity" =!= "NORMALE")
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", false)
      .start()

    //Détection automatique des anomalies critiques
    val alertQuery = transformed
      .filter($"severity" === "DANGER")
      .writeStream
      .foreachBatch((batchDF: org.apache.spark.sql.DataFrame, batchId: Long) => {
        if (!batchDF.isEmpty) {
          println("\n Seuil de pollution dépassé")
          println(s" -> Batch : $batchId")
          batchDF.show(false)
        }
      })
      .start()


    query.awaitTermination()
    alertQuery.awaitTermination()
  }
}
