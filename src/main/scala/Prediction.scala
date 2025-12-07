import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.regression.{RandomForestRegressor, LinearRegression, DecisionTreeRegressor}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator

object Prediction {

  /**
   * Analyse les corrélations entre PM2.5 et les autres variables
   */
  def analyzeCorrelations(df: DataFrame): Unit = {
    println("\n" + "=" * 70)
    println(" ANALYSE DES CORRÉLATIONS AVEC PM2.5")
    println("=" * 70)

    val correlations = df.select(
      corr("pm25", "pm10").alias("pm10"),
      corr("pm25", "co2").alias("co2"),
      corr("pm25", "no2").alias("no2"),
      corr("pm25", "o3").alias("o3"),
      corr("pm25", "temperature").alias("temperature"),
      corr("pm25", "humidity").alias("humidity"),
      corr("pm25", "wind_speed").alias("wind_speed"),
      corr("pm25", "noise").alias("noise")
    )

    correlations.show(truncate = false)
  }

  /**
   * Affiche les statistiques descriptives de PM2.5
   */
  def analyzePM25Distribution(df: DataFrame): Unit = {
    println("\n" + "=" * 70)
    println(" DISTRIBUTION DE PM2.5")
    println("=" * 70)

    df.describe("pm25").show()

    println("\n Top 10 des valeurs de PM2.5 les plus fréquentes :")
    df.groupBy("pm25")
      .count()
      .orderBy(desc("count"))
      .show(10)
  }

  /**
   * Construit et évalue les modèles de prédiction
   */
  def buildAndEvaluateModels(df: DataFrame, spark: SparkSession): Unit = {

    import spark.implicits._

    println("\n" + "=" * 70)
    println(" PARTIE 5 - PRÉDICTION DE LA POLLUTION")
    println("=" * 70)

    // Analyse préliminaire
    analyzeCorrelations(df)
    analyzePM25Distribution(df)

    // Encodage des variables catégorielles
    val lineIndexer = new StringIndexer()
      .setInputCol("line")
      .setOutputCol("line_indexed")
      .setHandleInvalid("keep")

    val cityIndexer = new StringIndexer()
      .setInputCol("city")
      .setOutputCol("city_indexed")
      .setHandleInvalid("keep")

    // Sélection des features pour la prédiction
    val featureCols = Array(
      "hour", "day_of_week", "month",
      "pm10", "co2", "no2", "o3",
      "noise", "humidity", "temperature", "wind_speed", "rain",
      "line_indexed", "city_indexed"
    )

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
      .setHandleInvalid("skip")

    // Préparation du dataset pour l'entraînement
    val dfForML = df
      .na.drop() // Supprimer les valeurs manquantes
      .withColumnRenamed("pm25", "label") // La variable cible

    // Division train/test (80/20)
    val Array(trainData, testData) = dfForML.randomSplit(Array(0.8, 0.2), seed = 42)

    println(s"\n Données d'entraînement : ${trainData.count()} lignes")
    println(s" Données de test : ${testData.count()} lignes")

    // ----- Modèle 1 : Régression Linéaire -----
    println("\n" + "-" * 70)
    println(" MODÈLE 1 : RÉGRESSION LINÉAIRE")
    println("-" * 70)

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.01)
      .setElasticNetParam(0.0)

    val pipelineLR = new Pipeline()
      .setStages(Array(lineIndexer, cityIndexer, assembler, lr))

    val modelLR = pipelineLR.fit(trainData)
    val predictionsLR = modelLR.transform(testData)

    val evaluatorRMSE = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val evaluatorR2 = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("r2")

    val evaluatorMAE = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("mae")

    val rmseLR = evaluatorRMSE.evaluate(predictionsLR)
    val r2LR = evaluatorR2.evaluate(predictionsLR)
    val maeLR = evaluatorMAE.evaluate(predictionsLR)

    println(f" RMSE : $rmseLR%.2f")
    println(f" R²   : $r2LR%.4f")
    println(f" MAE  : $maeLR%.2f")

    println("\n Exemples de prédictions :")
    predictionsLR.select("label", "prediction")
      .show(10, truncate = false)

    // ----- Modèle 2 : Arbre de Décision -----
    println("\n" + "-" * 70)
    println(" MODÈLE 2 : ARBRE DE DÉCISION")
    println("-" * 70)

    val dt = new DecisionTreeRegressor()
      .setMaxDepth(10)
      .setMaxBins(32)

    val pipelineDT = new Pipeline()
      .setStages(Array(lineIndexer, cityIndexer, assembler, dt))

    val modelDT = pipelineDT.fit(trainData)
    val predictionsDT = modelDT.transform(testData)

    val rmseDT = evaluatorRMSE.evaluate(predictionsDT)
    val r2DT = evaluatorR2.evaluate(predictionsDT)
    val maeDT = evaluatorMAE.evaluate(predictionsDT)

    println(f" RMSE : $rmseDT%.2f")
    println(f" R²   : $r2DT%.4f")
    println(f" MAE  : $maeDT%.2f")

    println("\n Exemples de prédictions :")
    predictionsDT.select("label", "prediction")
      .show(10, truncate = false)

    // ----- Modèle 3 : Forêt Aléatoire (Random Forest) -----
    println("\n" + "-" * 70)
    println(" MODÈLE 3 : FORÊT ALÉATOIRE (RANDOM FOREST)")
    println("-" * 70)

    val rf = new RandomForestRegressor()
      .setNumTrees(20)
      .setMaxDepth(10)
      .setMaxBins(32)
      .setSeed(42)

    val pipelineRF = new Pipeline()
      .setStages(Array(lineIndexer, cityIndexer, assembler, rf))

    val modelRF = pipelineRF.fit(trainData)
    val predictionsRF = modelRF.transform(testData)

    val rmseRF = evaluatorRMSE.evaluate(predictionsRF)
    val r2RF = evaluatorR2.evaluate(predictionsRF)
    val maeRF = evaluatorMAE.evaluate(predictionsRF)

    println(f" RMSE : $rmseRF%.2f")
    println(f" R²   : $r2RF%.4f")
    println(f" MAE  : $maeRF%.2f")

    println("\n Exemples de prédictions :")
    predictionsRF.select("label", "prediction")
      .show(10, truncate = false)

    // ----- Comparaison des modèles -----
    println("\n" + "=" * 70)
    println(" COMPARAISON DES MODÈLES")
    println("=" * 70)
    println(f" Régression Linéaire  - RMSE: $rmseLR%6.2f  R²: $r2LR%7.4f  MAE: $maeLR%6.2f")
    println(f" Arbre de Décision    - RMSE: $rmseDT%6.2f  R²: $r2DT%7.4f  MAE: $maeDT%6.2f")
    println(f" Random Forest        - RMSE: $rmseRF%6.2f  R²: $r2RF%7.4f  MAE: $maeRF%6.2f")
    println("=" * 70)

    // Déterminer le meilleur modèle
    val models = Seq(
      ("Régression Linéaire", r2LR),
      ("Arbre de Décision", r2DT),
      ("Random Forest", r2RF)
    )
    val bestModel = models.maxBy(_._2)
    println(s"\n Meilleur modèle : ${bestModel._1} (R² = ${bestModel._2}%.4f)")

    // Importance des features (Random Forest)
    println("\n" + "=" * 70)
    println(" IMPORTANCE DES FEATURES (RANDOM FOREST)")
    println("=" * 70)

    val rfModel = modelRF.stages.last.asInstanceOf[org.apache.spark.ml.regression.RandomForestRegressionModel]
    val featureImportances = rfModel.featureImportances.toArray

    println("\n Top 10 des features les plus importantes :")
    featureCols.zip(featureImportances)
      .sortBy(-_._2)
      .take(10)
      .zipWithIndex
      .foreach { case ((feature, importance), index) =>
        println(f" ${index + 1}%2d. $feature%-20s : ${importance * 100}%6.2f%%")
      }
  }

  /**
   * Point d'entrée pour la prédiction - peut être appelé depuis Main
   */
  def run(dfClean: DataFrame, spark: SparkSession): Unit = {
    buildAndEvaluateModels(dfClean, spark)
  }

  /**
   * Main pour exécuter la prédiction de façon autonome
   */
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("PredictionPollution")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    println("\n" + "=" * 70)
    println(" EXÉCUTION AUTONOME - MODULE PRÉDICTION")
    println("=" * 70)

    // Lecture des données
    println("\n Lecture du dataset...")
    val dfRaw = Loader.readCSV(spark, "data/pollution_big.csv")
    println(s" Nombre de lignes brutes : ${dfRaw.count()}")

    // Nettoyage
    println("\n Nettoyage des données...")
    val dfClean = Cleaner.clean(dfRaw)
    println(s" Nombre de lignes après nettoyage : ${dfClean.count()}")

    // Exécution de la prédiction
    buildAndEvaluateModels(dfClean, spark)

    // Arrêt de Spark
    spark.stop()

    println("\n" + "=" * 70)
    println(" FIN DE L'EXÉCUTION")
    println("=" * 70)
  }
}