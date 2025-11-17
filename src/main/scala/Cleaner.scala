import org.apache.spark.sql.DataFrame

object Cleaner {

  // Supprime les doublons
  private val removeDuplicates: DataFrame => DataFrame =
    df => df.dropDuplicates()

  // Supprime les lignes avec au moins une valeur manquante
  private val removeMissing: DataFrame => DataFrame =
    df => df.na.drop()

  // Pipeline complet de nettoyage
  val clean: DataFrame => DataFrame = df =>
    df
      .transform(removeDuplicates)
      .transform(removeMissing)
}
