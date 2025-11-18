import org.apache.spark.sql.SparkSession

object TestSpark {

  def main(args: Array[String]): Unit = {

    println("Tentative de démarrage de Spark...")

    val spark = SparkSession.builder()
      .appName("Test Spark")
      .master("local[*]")

      .config("spark.hadoop.home.dir", "/")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("SUCCESS ! Spark est correctement installé !")
    println(s"Version Spark: ${spark.version}")
    println(s"Master: ${spark.sparkContext.master}")
    println(s"App Name: ${spark.sparkContext.appName}")

    spark.stop()

    println("\nTest terminé avec succès")
  }
}