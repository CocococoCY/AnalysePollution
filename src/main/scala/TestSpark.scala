import org.apache.spark.sql.SparkSession

object TestSpark {

  def main(args: Array[String]): Unit = {

    println("Tentative de démarrage de Spark...")

    // Configuration Spark optimisée pour Windows
    val spark = SparkSession.builder()
      .appName("Test Spark")
      .master("local[*]")
      // Désactiver les logs Hadoop pour Windows
      .config("spark.hadoop.home.dir", "/")
      .getOrCreate()

    // Réduire la verbosité des logs
    spark.sparkContext.setLogLevel("WARN")

    println("\n" + "=" * 60)
    println("SUCCESS ! Spark est correctement installé !")
    println("=" * 60)
    println(s"Version Spark: ${spark.version}")
    println(s"Master: ${spark.sparkContext.master}")
    println(s"App Name: ${spark.sparkContext.appName}")
    println("=" * 60)

    // Arrêter Spark proprement
    spark.stop()

    println("\nTest terminé avec succès")
  }
}