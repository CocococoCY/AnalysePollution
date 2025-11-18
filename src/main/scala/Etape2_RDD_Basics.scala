import org.apache.spark.sql.SparkSession

object Etape2_RDD_Basics {

  def main(args: Array[String]): Unit = {

    // Créer la SparkSession
    val spark = SparkSession.builder()
      .appName("Étape 2 - RDD Basics")
      .master("local[*]")
      .config("spark.hadoop.home.dir", "/")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n" + "=" * 70)
    println("🎓 ÉTAPE 2 : Apprendre les RDD et la programmation fonctionnelle")
    println("=" * 70)

    // ========== PARTIE 1 : QU'EST-CE QU'UN RDD ? ==========
    println("\n📚 PARTIE 1 : Créer ton premier RDD")
    println("-" * 70)

    // Un RDD = Resilient Distributed Dataset
    // C'est une collection distribuée et immuable

    // Créer un RDD à partir d'une liste (données de pollution simulées)
    val mesuresCO2 = Seq(450.5, 520.3, 380.2, 610.8, 490.1, 720.5, 410.0, 580.3)
    val rdd = spark.sparkContext.parallelize(mesuresCO2)

    println("✅ RDD créé avec des mesures de CO2 (en ppm)")
    println(s"   Nombre de mesures : ${rdd.count()}")
    println(s"   Premières valeurs : ${rdd.take(3).mkString(", ")}")

    // ========== PARTIE 2 : MAP (Transformer chaque élément) ==========
    println("\n📚 PARTIE 2 : MAP - Transformer chaque élément")
    println("-" * 70)

    // MAP : Applique une fonction à CHAQUE élément
    // Exemple : Convertir ppm en mg/m³ (CO2)
    val rddEnMg = rdd.map(ppm => ppm * 1.8)  // Formule simplifiée

    println("🔄 MAP : Conversion ppm → mg/m³")
    println("   AVANT map :")
    rdd.take(3).foreach(v => println(f"   - $v%.1f ppm"))
    println("   APRÈS map :")
    rddEnMg.take(3).foreach(v => println(f"   - $v%.1f mg/m³"))

    // Autre exemple MAP : Classifier les niveaux
    val rddClassification = rdd.map { co2 =>
      if (co2 < 450) "🟢 Bon"
      else if (co2 < 600) "🟡 Moyen"
      else "🔴 Mauvais"
    }

    println("\n🔄 MAP : Classification des niveaux")
    rdd.zip(rddClassification).take(5).foreach { case (co2, classe) =>
      println(f"   CO2: $co2%.1f ppm → $classe")
    }

    // ========== PARTIE 3 : FILTER (Sélectionner des éléments) ==========
    println("\n📚 PARTIE 3 : FILTER - Sélectionner des éléments")
    println("-" * 70)

    // FILTER : Garde seulement les éléments qui satisfont une condition
    val pollutionElevee = rdd.filter(co2 => co2 > 500)

    println(s"🔍 FILTER : Garder seulement CO2 > 500 ppm")
    println(s"   Total de mesures : ${rdd.count()}")
    println(s"   Mesures > 500 ppm : ${pollutionElevee.count()}")
    println("   Valeurs filtrées :")
    pollutionElevee.collect().foreach(v => println(f"   - $v%.1f ppm"))

    // ========== PARTIE 4 : REDUCE (Agréger en une valeur) ==========
    println("\n📚 PARTIE 4 : REDUCE - Agréger toutes les valeurs")
    println("-" * 70)

    // REDUCE : Combine tous les éléments pour obtenir UNE seule valeur

    // Exemple 1 : Trouver le maximum
    val maxCO2 = rdd.reduce((a, b) => if (a > b) a else b)
    println(s"📊 REDUCE : Maximum CO2 = $maxCO2 ppm")

    // Exemple 2 : Calculer la somme
    val sommeCO2 = rdd.reduce((a, b) => a + b)
    println(s"📊 REDUCE : Somme totale = ${sommeCO2.toInt} ppm")

    // Exemple 3 : Calculer la moyenne (somme / count)
    val moyenneCO2 = sommeCO2 / rdd.count()
    println(f"📊 REDUCE : Moyenne = $moyenneCO2%.1f ppm")

    // ========== PARTIE 5 : CHAÎNER LES OPÉRATIONS ==========
    println("\n📚 PARTIE 5 : CHAÎNER les opérations (le vrai pouvoir !)")
    println("-" * 70)

    // On peut enchaîner map, filter, reduce !
    val pollutionCritique = rdd
      .filter(co2 => co2 > 600)        // Garder seulement > 600
      .map(co2 => co2 * 1.8)           // Convertir en mg/m³
      .reduce((a, b) => a + b)         // Sommer

    println("🔗 Chaînage : filter → map → reduce")
    println(s"   Résultat : Somme des valeurs > 600 ppm (en mg/m³) = ${pollutionCritique.toInt}")

    // ========== PARTIE 6 : EXEMPLE RÉALISTE ==========
    println("\n📚 PARTIE 6 : Exemple avec des données de stations")
    println("-" * 70)

    // Créer des données plus réalistes : (Station, CO2)
    val donnees = Seq(
      ("Station_A", 450.5),
      ("Station_B", 620.3),
      ("Station_A", 480.2),
      ("Station_C", 710.8),
      ("Station_B", 590.1),
      ("Station_A", 420.5),
      ("Station_C", 680.0)
    )

    val rddStations = spark.sparkContext.parallelize(donnees)

    // Question : Quelle est la pollution moyenne de Station_A ?
    val stationA_moyenne = rddStations
      .filter { case (station, co2) => station == "Station_A" }  // Garder Station_A
      .map { case (station, co2) => co2 }                        // Extraire juste CO2
      .reduce((a, b) => a + b) / 3                               // Moyenne (3 mesures)

    println(f"🏭 Pollution moyenne Station_A : $stationA_moyenne%.1f ppm")

    // Question : Combien de stations ont dépassé 600 ppm ?
    val stationsProbleme = rddStations
      .filter { case (station, co2) => co2 > 600 }
      .map { case (station, co2) => station }
      .distinct()  // Enlever les doublons
      .count()

    println(s"⚠️  Nombre de stations ayant dépassé 600 ppm : $stationsProbleme")

    // ========== RÉSUMÉ ==========
    println("\n" + "=" * 70)
    println("📝 RÉSUMÉ DES CONCEPTS")
    println("=" * 70)
    println("""
    ✅ RDD = Collection distribuée et immuable
    
    ✅ MAP    : Transforme CHAQUE élément
                Exemple : rdd.map(x => x * 2)
    
    ✅ FILTER : Garde seulement certains éléments
                Exemple : rdd.filter(x => x > 100)
    
    ✅ REDUCE : Combine tous les éléments en UN seul
                Exemple : rdd.reduce((a, b) => a + b)
    
    ✅ On peut CHAÎNER : rdd.filter(...).map(...).reduce(...)
    
    💡 Tout est LAZY : Spark ne calcule que quand on appelle
       une ACTION (count, collect, reduce, take...)
    """)

    println("\n🎉 Bravo ! Tu maîtrises maintenant les bases des RDD !")
    println("   Prochaine étape : GroupBy et les opérations avancées !")

    spark.stop()
  }
}