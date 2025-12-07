import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import java.io._

object GraphAnalysis {

  // Seuils OMS pour normalisation
  val seuilPM25 = 25.0
  val seuilPM10 = 50.0
  val seuilNO2 = 40.0
  val seuilO3 = 100.0
  val seuilCO2 = 1000.0

  /**
   * Calcul de l'indice de pollution normalisé (entre 0 et ~2)
   * Formule cohérente avec Stationspollution.scala
   */
  def calculerIndicePollution(pm25: Int, pm10: Int, co2: Int, no2: Int, o3: Int): Double = {
    (
      (pm25.toDouble / seuilPM25) * 3 +
        (pm10.toDouble / seuilPM10) * 2 +
        (co2.toDouble / seuilCO2) * 0.5 +
        (no2.toDouble / seuilNO2) * 2 +
        (o3.toDouble / seuilO3) * 1
      ) / 8.5
  }

  def buildAndAnalyzeGraph(dfClean: DataFrame)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    println("\n" + "="*60)
    println(" ANALYSE DE GRAPHE - RÉSEAU DE POLLUTION URBAIN")
    println("="*60)

    // 1. Calcul de l'indice de pollution pour chaque station
    val stationsWithIndex = dfClean
      .select("station_id", "station_name", "city", "latitude", "longitude", "line",
        "pm25", "pm10", "co2", "no2", "o3")
      .rdd
      .map { row =>
        val stationId = row.getAs[Int]("station_id")
        val stationName = row.getAs[String]("station_name")
        val city = row.getAs[String]("city")
        val latitude = row.getAs[Double]("latitude")
        val longitude = row.getAs[Double]("longitude")
        val line = row.getAs[String]("line")
        val pm25 = row.getAs[Int]("pm25")
        val pm10 = row.getAs[Int]("pm10")
        val co2 = row.getAs[Int]("co2")
        val no2 = row.getAs[Int]("no2")
        val o3 = row.getAs[Int]("o3")

        val pollutionIndex = calculerIndicePollution(pm25, pm10, co2, no2, o3)

        ((stationId, stationName, city, latitude, longitude), (line, pollutionIndex))
      }

    // 2. Agréger par station pour calculer l'indice moyen
    val stationStats = stationsWithIndex
      .aggregateByKey((0.0, 0, Set.empty[String]))(
        { case ((sum, count, lines), (line, pollution)) =>
          (sum + pollution, count + 1, lines + line)
        },
        { case ((sum1, count1, lines1), (sum2, count2, lines2)) =>
          (sum1 + sum2, count1 + count2, lines1 ++ lines2)
        }
      )
      .map { case ((id, name, city, lat, lon), (sum, count, lines)) =>
        val avgPollution = sum / count
        (id, name, city, lat, lon, avgPollution, lines)
      }

    // 3. Conversion en Vertex RDD
    val vertices: RDD[(VertexId, (String, String, Double, Double, Double, Set[String]))] =
      stationStats.map { case (id, name, city, lat, lon, pollution, lines) =>
        (id.toLong, (name, city, lat, lon, pollution, lines))
      }

    // 4. Construire les arêtes à partir des lignes de métro
    val stationPerLine = dfClean
      .select("line", "station_id", "city")
      .distinct()
      .rdd
      .groupBy(row => (row.getAs[String]("line"), row.getAs[String]("city")))

    val edges: RDD[Edge[String]] = stationPerLine.flatMap { case ((line, city), rows) =>
      val sorted = rows.map(_.getAs[Int]("station_id")).toList.sorted
      sorted.sliding(2).collect { case List(a, b) =>
        Edge(a.toLong, b.toLong, line)
      }
    }

    // 5. Construction du graphe
    val graph = Graph(vertices, edges)

    println(s"\n Statistiques du graphe :")
    println(s"   • Nombre de stations (vertices) : ${graph.vertices.count()}")
    println(s"   • Nombre de connexions (edges)  : ${graph.edges.count()}")

    // 6. Top 10 stations les plus polluées
    println("\n === Top 10 Stations les plus polluées ===")
    val top10Polluted = graph.vertices
      .map { case (id, (name, city, _, _, pollution, _)) => (id, name, city, pollution) }
      .sortBy(_._4, ascending = false)
      .take(10)

    top10Polluted.zipWithIndex.foreach { case ((id, name, city, pollution), idx) =>
      println(f"   ${idx + 1}. $name%-20s ($city%-15s) : $pollution%.3f")
    }

    // 7. Analyse de propagation avec Pregel
    println("\n === Analyse de propagation de pollution (Pregel) ===")
    val initialGraph = graph.mapVertices { case (id, (name, city, lat, lon, pollution, lines)) =>
      (name, city, lat, lon, pollution, pollution, lines) // (info, pollution_initiale, pollution_propagée)
    }

    val propagation = initialGraph.pregel(
      initialMsg = 0.0,
      maxIterations = 5,
      activeDirection = EdgeDirection.Either
    )(
      // Vertex Program : mise à jour de la pollution propagée
      (id, attr, msg) => {
        val (name, city, lat, lon, initialPollution, currentPollution, lines) = attr
        val newPollution = currentPollution + msg * 0.1
        (name, city, lat, lon, initialPollution, newPollution, lines)
      },
      // Send Message : envoie la pollution aux voisins
      triplet => {
        val srcPollution = triplet.srcAttr._6
        val dstPollution = triplet.dstAttr._6
        Iterator(
          (triplet.dstId, srcPollution),
          (triplet.srcId, dstPollution)
        )
      },
      // Merge Message : combine les messages
      (a, b) => a + b
    )

    val topPropagated = propagation.vertices
      .map { case (id, (name, city, _, _, initial, propagated, _)) =>
        (id, name, city, initial, propagated, propagated - initial)
      }
      .sortBy(_._6, ascending = false)
      .take(10)

    println("   Top 10 stations impactées par la propagation :")
    topPropagated.zipWithIndex.foreach { case ((id, name, city, initial, propagated, increase), idx) =>
      println(f"   ${idx + 1}. $name%-20s : Initial=$initial%.3f → Propagée=$propagated%.3f (+$increase%.3f)")
    }

    // 8. Stations critiques (pollution × degré de connectivité)
    println("\n  === Stations critiques (Pollution × Degré de connectivité) ===")
    val degrees = graph.degrees
    val critical = graph.vertices
      .join(degrees)
      .map { case (id, ((name, city, _, _, pollution, _), degree)) =>
        (id, name, city, pollution, degree, pollution * degree)
      }
      .sortBy(_._6, ascending = false)
      .take(10)

    critical.zipWithIndex.foreach { case ((id, name, city, pollution, degree, score), idx) =>
      println(f"   ${idx + 1}. $name%-20s : Score=$score%.2f (Pollution=$pollution%.3f × Degré=$degree)")
    }

    // 9. Composantes connexes (clusters de stations)
    println("\n === Analyse des composantes connexes ===")
    val cc = graph.connectedComponents()
    val componentSizes = cc.vertices
      .map { case (id, componentId) => (componentId, 1) }
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .collect()

    println(s"   Nombre de composantes connexes : ${componentSizes.length}")
    println("   Taille des principales composantes :")
    componentSizes.take(5).zipWithIndex.foreach { case ((compId, size), idx) =>
      println(f"   ${idx + 1}. Composante $compId : $size stations")
    }

    // 10. Export des visualisations D3.js PAR VILLE UNIQUEMENT
    println("\n === Export des visualisations par ville ===")

    val cities = graph.vertices
      .map { case (_, (_, city, _, _, _, _)) => city }
      .distinct()
      .collect()

    println(s"\n   Villes détectées : ${cities.mkString(", ")}")

    // Créer le dossier output s'il n'existe pas
    new File("output").mkdirs()

    cities.foreach { city =>
      exportToD3ByCity(graph, city, s"output/metro_network_$city")
    }

    println("\n Analyse GraphX terminée avec succès !")
    println("    Fichiers HTML générés dans le dossier 'output/'")
    println("="*60 + "\n")
  }

  /**
   * Export du graphe par ville avec coordonnées GPS FIXES
   */
  def exportToD3ByCity(graph: Graph[(String, String, Double, Double, Double, Set[String]), String],
                       cityName: String,
                       filename: String): Unit = {

    // Filtrer les nœuds de la ville
    val cityVertices = graph.vertices.filter { case (_, (_, city, _, _, _, _)) => city == cityName }
    val cityVertexIds = cityVertices.map(_._1).collect().toSet

    // Filtrer les arêtes de la ville
    val cityEdges = graph.edges.filter { e =>
      cityVertexIds.contains(e.srcId) && cityVertexIds.contains(e.dstId)
    }

    val nodesData = cityVertices.collect().map { case (id, (name, city, lat, lon, pollution, lines)) =>
      val linesStr = lines.mkString(", ")
      s"""{"id": $id, "name": "$name", "city": "$city", "lat": $lat, "lon": $lon, "pollution": $pollution, "lines": "$linesStr"}"""
    }.mkString(",\n      ")

    val edgesData = cityEdges.collect().map { e =>
      s"""{"source": ${e.srcId}, "target": ${e.dstId}, "line": "${e.attr}"}"""
    }.mkString(",\n      ")

    val html = generateD3HTML(nodesData, edgesData, s"Réseau de pollution - $cityName")

    saveToFile(html, s"$filename.html")
    println(s"   ✓ Graphe $cityName exporté : $filename.html")
  }

  /**
   * Génère le code HTML avec D3.js - POSITIONS FIXES BASÉES SUR GPS
   */
  def generateD3HTML(nodesJson: String, edgesJson: String, title: String): String = {
    s"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>$title</title>
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <style>
    * {
      margin: 0;
      padding: 0;
      box-sizing: border-box;
    }

    body {
      font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      overflow: hidden;
    }

    #container {
      display: flex;
      height: 100vh;
    }

    #sidebar {
      width: 320px;
      background: white;
      padding: 20px;
      box-shadow: 2px 0 10px rgba(0,0,0,0.1);
      overflow-y: auto;
    }

    h1 {
      color: #333;
      font-size: 24px;
      margin-bottom: 20px;
      border-bottom: 3px solid #667eea;
      padding-bottom: 10px;
    }

    h2 {
      color: #555;
      font-size: 18px;
      margin-top: 25px;
      margin-bottom: 15px;
    }

    #graph {
      flex: 1;
      position: relative;
      background: #f8f9fa;
    }

    .stat {
      background: #f0f0f0;
      padding: 12px;
      margin: 10px 0;
      border-radius: 8px;
      border-left: 4px solid #667eea;
    }

    .stat-label {
      font-size: 12px;
      color: #666;
      margin-bottom: 5px;
    }

    .stat-value {
      font-size: 20px;
      font-weight: bold;
      color: #333;
    }

    .legend {
      margin: 15px 0;
    }

    .legend-item {
      display: flex;
      align-items: center;
      margin: 10px 0;
      font-size: 13px;
    }

    .legend-color {
      width: 20px;
      height: 20px;
      border-radius: 50%;
      margin-right: 10px;
      border: 2px solid #fff;
      box-shadow: 0 2px 4px rgba(0,0,0,0.2);
    }

    .node circle {
      cursor: pointer;
      transition: all 0.3s;
    }

    .node:hover circle {
      stroke-width: 4px !important;
      filter: brightness(1.1);
    }

    .link {
      stroke: #999;
      stroke-opacity: 0.6;
      stroke-width: 2px;
    }

    .link.highlighted {
      stroke: #667eea;
      stroke-opacity: 1;
      stroke-width: 3px;
    }

    text {
      font: 12px sans-serif;
      pointer-events: none;
      text-shadow: 0 1px 0 #fff, 1px 0 0 #fff, 0 -1px 0 #fff, -1px 0 0 #fff;
    }

    .tooltip {
      position: absolute;
      background: white;
      border: 2px solid #667eea;
      border-radius: 8px;
      padding: 12px;
      pointer-events: none;
      opacity: 0;
      transition: opacity 0.3s;
      box-shadow: 0 4px 12px rgba(0,0,0,0.15);
      font-size: 13px;
      max-width: 250px;
    }

    .tooltip strong {
      color: #667eea;
      font-size: 15px;
    }

    .controls {
      margin: 20px 0;
    }

    button {
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
      border: none;
      padding: 12px 16px;
      border-radius: 8px;
      cursor: pointer;
      margin: 5px 0;
      width: 100%;
      font-size: 14px;
      font-weight: 600;
      transition: transform 0.2s, box-shadow 0.2s;
    }

    button:hover {
      transform: translateY(-2px);
      box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
    }

    button:active {
      transform: translateY(0);
    }

    ul {
      padding-left: 20px;
    }

    li {
      margin: 8px 0;
      line-height: 1.5;
    }
  </style>
</head>
<body>
  <div id="container">
    <div id="sidebar">
      <h1>$title</h1>

      <h2> Statistiques</h2>
      <div class="stat">
        <div class="stat-label">Nombre de stations</div>
        <div class="stat-value" id="stat-nodes">-</div>
      </div>
      <div class="stat">
        <div class="stat-label">Nombre de connexions</div>
        <div class="stat-value" id="stat-edges">-</div>
      </div>
      <div class="stat">
        <div class="stat-label">Pollution moyenne</div>
        <div class="stat-value" id="stat-pollution">-</div>
      </div>
      <div class="stat">
        <div class="stat-label">Pollution maximale</div>
        <div class="stat-value" id="stat-max-pollution">-</div>
      </div>

      <h2> Légende</h2>
      <div class="legend">
        <div class="legend-item">
          <div class="legend-color" style="background: #2ecc71;"></div>
          <span>Faible pollution (&lt; 0.5)</span>
        </div>
        <div class="legend-item">
          <div class="legend-color" style="background: #f39c12;"></div>
          <span>Pollution moyenne (0.5-1.0)</span>
        </div>
        <div class="legend-item">
          <div class="legend-color" style="background: #e74c3c;"></div>
          <span>Pollution élevée (&gt; 1.0)</span>
        </div>
      </div>

      <h2> Contrôles</h2>
      <div class="controls">
        <button onclick="toggleLabels()">️ Afficher/Masquer labels</button>
        <button onclick="highlightHighPollution()">⚠ Mettre en évidence pollution élevée</button>
        <button onclick="resetHighlight()"> Réinitialiser mise en évidence</button>
      </div>

      <h2> Instructions</h2>
      <ul style="font-size: 12px; color: #666;">
        <li>Les stations sont positionnées selon leurs coordonnées GPS réelles</li>
        <li>Survolez un nœud pour voir les détails</li>
        <li>La taille du nœud représente le niveau de pollution</li>
        <li>La couleur indique l'intensité de la pollution</li>
      </ul>
    </div>

    <div id="graph">
      <svg id="svg"></svg>
      <div class="tooltip" id="tooltip"></div>
    </div>
  </div>

  <script>
    const width = window.innerWidth - 320;
    const height = window.innerHeight;

    const nodes = [
      $nodesJson
    ];

    const links = [
      $edgesJson
    ];

    // Statistiques
    document.getElementById('stat-nodes').textContent = nodes.length;
    document.getElementById('stat-edges').textContent = links.length;
    const avgPollution = d3.mean(nodes, d => d.pollution);
    const maxPollution = d3.max(nodes, d => d.pollution);
    document.getElementById('stat-pollution').textContent = avgPollution.toFixed(3);
    document.getElementById('stat-max-pollution').textContent = maxPollution.toFixed(3);

    const svg = d3.select("#svg")
      .attr("width", width)
      .attr("height", height);

    const tooltip = d3.select("#tooltip");

    // Échelle de couleur basée sur les nouveaux seuils
    const colorScale = d3.scaleLinear()
      .domain([0, 0.5, 1.0, d3.max(nodes, d => d.pollution)])
      .range(["#2ecc71", "#f39c12", "#e74c3c", "#c0392b"]);

    // Positionnement FIXE basé sur les coordonnées GPS
    const latExtent = d3.extent(nodes, d => d.lat);
    const lonExtent = d3.extent(nodes, d => d.lon);

    const xScale = d3.scaleLinear()
      .domain(lonExtent)
      .range([80, width - 80]);

    const yScale = d3.scaleLinear()
      .domain(latExtent)
      .range([height - 80, 80]);

    // Positionner les nœuds et FIXER leurs positions
    nodes.forEach(d => {
      d.x = xScale(d.lon);
      d.y = yScale(d.lat);
      d.fx = d.x;  // Position X fixe
      d.fy = d.y;  // Position Y fixe
    });

    // Simulation avec forces minimales (juste pour les liens)
    const simulation = d3.forceSimulation(nodes)
      .force("link", d3.forceLink(links).id(d => d.id).distance(50).strength(0.1))
      .force("charge", d3.forceManyBody().strength(-50))
      .force("collide", d3.forceCollide().radius(d => Math.sqrt(d.pollution) * 20 + 10));

    // Arêtes
    const link = svg.append("g")
      .selectAll("line")
      .data(links)
      .enter().append("line")
      .attr("class", "link");

    // Nœuds
    const node = svg.append("g")
      .selectAll("g")
      .data(nodes)
      .enter().append("g")
      .attr("class", "node")
      .on("mouseover", showTooltip)
      .on("mouseout", hideTooltip);

    node.append("circle")
      .attr("r", d => Math.sqrt(d.pollution) * 20 + 8)
      .attr("fill", d => colorScale(d.pollution))
      .attr("stroke", "#fff")
      .attr("stroke-width", 2.5);

    const labels = node.append("text")
      .attr("dx", 12)
      .attr("dy", ".35em")
      .text(d => d.name)
      .attr("class", "label")
      .style("font-weight", "600");

    // Animation (légère, sans déplacement des nœuds)
    simulation.on("tick", () => {
      link
        .attr("x1", d => d.source.x)
        .attr("y1", d => d.source.y)
        .attr("x2", d => d.target.x)
        .attr("y2", d => d.target.y);

      node.attr("transform", d => `translate($${d.x},$${d.y})`);
    });

    // Arrêter la simulation rapidement pour éviter tout mouvement
    simulation.alpha(0.1).alphaTarget(0);

    function showTooltip(event, d) {
      tooltip
        .style("opacity", 1)
        .style("left", (event.pageX + 10) + "px")
        .style("top", (event.pageY - 10) + "px")
        .html(`
          <strong>$${d.name}</strong><br>
          <strong>Ville:</strong> $${d.city}<br>
          <strong>Lignes:</strong> $${d.lines}<br>
          <strong>Indice de pollution:</strong> <span style="color: $${colorScale(d.pollution)}; font-weight: bold;">$${d.pollution.toFixed(3)}</span><br>
          <strong>Coordonnées:</strong> $${d.lat.toFixed(4)}, $${d.lon.toFixed(4)}
        `);

      // Mettre en évidence les liens connectés
      link.classed("highlighted", l =>
        l.source.id === d.id || l.target.id === d.id
      );
    }

    function hideTooltip() {
      tooltip.style("opacity", 0);
      link.classed("highlighted", false);
    }

    let labelsVisible = true;
    function toggleLabels() {
      labelsVisible = !labelsVisible;
      labels.style("opacity", labelsVisible ? 1 : 0);
    }

    function highlightHighPollution() {
      const threshold = d3.quantile(nodes.map(d => d.pollution), 0.75);
      node.select("circle")
        .transition()
        .duration(500)
        .attr("stroke", d => d.pollution > threshold ? "#ff0000" : "#fff")
        .attr("stroke-width", d => d.pollution > threshold ? 5 : 2.5);
    }

    function resetHighlight() {
      node.select("circle")
        .transition()
        .duration(500)
        .attr("stroke", "#fff")
        .attr("stroke-width", 2.5);
    }
  </script>
</body>
</html>"""
  }

  /**
   * Sauvegarde dans un fichier
   */
  def saveToFile(content: String, filepath: String): Unit = {
    val file = new File(filepath)
    file.getParentFile.mkdirs()
    val writer = new PrintWriter(file)
    try {
      writer.write(content)
    } finally {
      writer.close()
    }
  }
}