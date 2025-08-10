package com.taxi.analytics.graph

import java.util.function.{Function => JFunction}
import java.util.{List => JList}

import scala.collection.JavaConverters._

import com.taxi.analytics.database.{Neo4jConnector, ParameterEncoder}
import com.typesafe.scalalogging.StrictLogging
import org.neo4j.driver.{Record, TransactionContext, Value}

// ---------------------------------------------------------------------------
// Real-Time Graph Analytics & NYC Taxi Demand Forecasting
// Centrality pipeline over projected zone graphs
//
// Two complementary notions of zone importance are computed against the
// in-memory projections built by GraphProjectionBuilder:
//
//   PageRank    — a zone matters when zones that matter send trips to it.
//                 Captures sustained demand pull; robust to noise.
//   Betweenness — a zone matters when it sits on the paths between other
//                 zones. Captures transfer corridors that carry little
//                 terminal demand of their own but whose disruption
//                 propagates widely.
//
// The two disagree by design, and the composite score exposes that rather
// than hiding it: a zone that ranks high on one and low on the other is the
// interesting case for the forecaster.
// ---------------------------------------------------------------------------

/** Tuning for the PageRank pass. */
final case class PageRankConfig(
    maxIterations: Int = 20,
    dampingFactor: Double = 0.85,
    tolerance: Double = 0.0000001,
    relationshipWeightProperty: Option[String] = Some("tripCount"),
    sourceLocationIds: Seq[Int] = Seq.empty,
    scaler: Option[String] = None,
    concurrency: Int = 4
) extends Serializable {

  require(maxIterations >= 1, "maxIterations must be at least 1")
  require(dampingFactor > 0.0 && dampingFactor < 1.0, "dampingFactor must be within (0.0, 1.0)")
  require(tolerance > 0.0, "tolerance must be positive")
  require(concurrency >= 1, "concurrency must be at least 1")

  relationshipWeightProperty.foreach(property =>
    CypherIdentifier.require("relationship weight property", property)
  )

  /** True when the run is personalised around a seed set of zones. */
  def isPersonalised: Boolean = sourceLocationIds.nonEmpty

  private[graph] def toMap: Map[String, Any] = {
    val base = Map[String, Any](
      "maxIterations" -> maxIterations,
      "dampingFactor" -> dampingFactor,
      "tolerance" -> tolerance,
      "concurrency" -> concurrency
    )
    val weighted = relationshipWeightProperty.fold(base)(property => base + ("relationshipWeightProperty" -> property))
    scaler.fold(weighted)(value => weighted + ("scaler" -> value))
  }

  def describe: String =
    s"iterations=$maxIterations damping=$dampingFactor tolerance=$tolerance " +
      s"weight=${relationshipWeightProperty.getOrElse("none")} personalised=$isPersonalised"
}

/** Tuning for the Betweenness Centrality pass. */
final case class BetweennessConfig(
    samplingSize: Option[Int] = None,
    samplingSeed: Option[Long] = None,
    relationshipWeightProperty: Option[String] = None,
    concurrency: Int = 4
) extends Serializable {

  require(samplingSize.forall(_ >= 1), "samplingSize must be at least 1 when set")
  require(concurrency >= 1, "concurrency must be at least 1")

  relationshipWeightProperty.foreach(property =>
    CypherIdentifier.require("relationship weight property", property)
  )

  /** Exact betweenness is only tractable on small graphs; sampling trades accuracy for time. */
  def isSampled: Boolean = samplingSize.isDefined

  private[graph] def toMap: Map[String, Any] = {
    val base = Map[String, Any]("concurrency" -> concurrency)
    val sampled = samplingSize.fold(base)(size => base + ("samplingSize" -> size))
    val seeded = samplingSeed.fold(sampled)(seed => sampled + ("samplingSeed" -> seed))
    relationshipWeightProperty.fold(seeded)(property => seeded + ("relationshipWeightProperty" -> property))
  }

  def describe: String =
    s"sampling=${samplingSize.map(_.toString).getOrElse("exact")} " +
      s"weight=${relationshipWeightProperty.getOrElse("none")} concurrency=$concurrency"
}

/** A single zone's score for one algorithm. */
final case class CentralityScore(
    locationId: Int,
    zoneName: String,
    borough: String,
    score: Double
)

/** Percentile summary GDS returns alongside a stats, mutate or write run. */
final case class CentralityDistribution(
    min: Double,
    mean: Double,
    max: Double,
    p50: Double,
    p75: Double,
    p90: Double,
    p95: Double,
    p99: Double,
    p999: Double,
    stdDev: Double
) {
  def summary: String =
    f"min=$min%.6f p50=$p50%.6f p90=$p90%.6f p99=$p99%.6f max=$max%.6f mean=$mean%.6f stddev=$stdDev%.6f"
}

object CentralityDistribution {

  val Empty: CentralityDistribution = CentralityDistribution(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

  private[graph] def fromValue(value: Value): CentralityDistribution =
    if (value == null || value.isNull) {
      Empty
    } else {
      val entries = value
        .asMap(new JFunction[Value, java.lang.Double] {
          override def apply(entry: Value): java.lang.Double =
            if (entry == null || entry.isNull) java.lang.Double.valueOf(0.0)
            else java.lang.Double.valueOf(entry.asDouble(0.0))
        })
        .asScala
        .toMap
        .map { case (key, boxed) => key -> boxed.doubleValue() }

      def at(key: String): Double = entries.getOrElse(key, 0.0)

      CentralityDistribution(
        min = at("min"),
        mean = at("mean"),
        max = at("max"),
        p50 = at("p50"),
        p75 = at("p75"),
        p90 = at("p90"),
        p95 = at("p95"),
        p99 = at("p99"),
        p999 = at("p999"),
        stdDev = at("stdDev")
      )
    }
}

/** Outcome of a non-streaming algorithm run. */
final case class CentralityRunSummary(
    algorithm: String,
    graphName: String,
    propertyName: String,
    nodePropertiesWritten: Long,
    ranIterations: Long,
    didConverge: Boolean,
    distribution: CentralityDistribution,
    computeMillis: Long,
    totalMillis: Long
) {
  def summary: String =
    s"algorithm=$algorithm graph=$graphName property=$propertyName nodes=$nodePropertiesWritten " +
      s"iterations=$ranIterations converged=$didConverge compute_ms=$computeMillis total_ms=$totalMillis"
}

/** Memory estimate for an algorithm against a materialised projection. */
final case class AlgorithmEstimate(
    algorithm: String,
    requiredMemory: String,
    bytesMin: Long,
    bytesMax: Long
) {
  def summary: String = s"algorithm=$algorithm required=$requiredMemory bytes_min=$bytesMin bytes_max=$bytesMax"
}

/** Relative weighting of the two centrality signals in the composite score. */
final case class ImportanceWeights(pageRank: Double, betweenness: Double) extends Serializable {

  require(pageRank >= 0.0 && betweenness >= 0.0, "importance weights must be non-negative")
  require(pageRank + betweenness > 0.0, "at least one importance weight must be positive")

  private val total: Double = pageRank + betweenness

  def normalisedPageRank: Double = pageRank / total
  def normalisedBetweenness: Double = betweenness / total
}

object ImportanceWeights {
  /** Demand pull dominates; corridor position is a secondary correction. */
  val Default: ImportanceWeights = ImportanceWeights(pageRank = 0.65, betweenness = 0.35)
  val Balanced: ImportanceWeights = ImportanceWeights(pageRank = 0.5, betweenness = 0.5)
  val CorridorFocused: ImportanceWeights = ImportanceWeights(pageRank = 0.3, betweenness = 0.7)
}

/** Combined importance record for one zone. */
final case class ZoneImportance(
    locationId: Int,
    zoneName: String,
    borough: String,
    pageRank: Double,
    betweenness: Double,
    normalisedPageRank: Double,
    normalisedBetweenness: Double,
    importanceScore: Double,
    importanceRank: Int
) {

  /**
   * Positive when a zone is a stronger corridor than its demand pull would
   * suggest — the signature of a transfer zone whose disruption propagates
   * further than its own trip volume implies.
   */
  def corridorBias: Double = normalisedBetweenness - normalisedPageRank

  private[graph] def toRow: Map[String, Any] = Map(
    "locationId" -> locationId,
    "pageRank" -> pageRank,
    "betweenness" -> betweenness,
    "normalisedPageRank" -> normalisedPageRank,
    "normalisedBetweenness" -> normalisedBetweenness,
    "importanceScore" -> importanceScore,
    "importanceRank" -> importanceRank,
    "corridorBias" -> corridorBias
  )
}

/** Raised when an algorithm run does not return the expected catalogue result. */
final class CentralityException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

/**
 * Executes centrality routines against projections held in the GDS catalogue.
 *
 * Every method takes the physical graph name, which callers normally obtain
 * from `GraphProjectionBuilder.refresh(...).graphName` or `activeGraphName`.
 * Nothing here creates or drops a projection: the pass is a pure consumer, so
 * a single projection can serve several algorithm runs.
 */
final class CentralityAnalytics(connector: Neo4jConnector) extends StrictLogging {

  import CentralityAnalytics._

  // -------------------------------------------------------------------------
  // PageRank
  // -------------------------------------------------------------------------

  /** Streams PageRank scores without persisting anything. */
  def pageRankStream(
      graphName: String,
      config: PageRankConfig = PageRankConfig(),
      limit: Int = 0
  ): Vector[CentralityScore] = {
    logger.info(s"PageRank stream on $graphName: ${config.describe}")
    streamScores("gds.pageRank", graphName, config.toMap, config.sourceLocationIds, limit)
  }

  /** Runs PageRank for its distribution only, materialising no properties. */
  def pageRankStats(graphName: String, config: PageRankConfig = PageRankConfig()): CentralityRunSummary =
    runNonStreaming(
      algorithm = "gds.pageRank",
      mode = "stats",
      graphName = graphName,
      propertyName = "",
      configuration = config.toMap,
      sourceLocationIds = config.sourceLocationIds
    )

  /**
   * Writes PageRank into the in-memory graph so a later pass can consume it
   * without a round trip through the store.
   */
  def pageRankMutate(
      graphName: String,
      config: PageRankConfig = PageRankConfig(),
      mutateProperty: String = DefaultPageRankProperty
  ): CentralityRunSummary = {
    val property = CypherIdentifier.require("mutate property", mutateProperty)
    runNonStreaming(
      algorithm = "gds.pageRank",
      mode = "mutate",
      graphName = graphName,
      propertyName = property,
      configuration = config.toMap + ("mutateProperty" -> property),
      sourceLocationIds = config.sourceLocationIds
    )
  }

  /** Persists PageRank onto the stored `:Location` nodes. */
  def pageRankWrite(
      graphName: String,
      config: PageRankConfig = PageRankConfig(),
      writeProperty: String = DefaultPageRankProperty,
      writeConcurrency: Int = 4
  ): CentralityRunSummary = {
    val property = CypherIdentifier.require("write property", writeProperty)
    runNonStreaming(
      algorithm = "gds.pageRank",
      mode = "write",
      graphName = graphName,
      propertyName = property,
      configuration = config.toMap ++ Map("writeProperty" -> property, "writeConcurrency" -> writeConcurrency),
      sourceLocationIds = config.sourceLocationIds
    )
  }

  def pageRankEstimate(graphName: String, config: PageRankConfig = PageRankConfig()): AlgorithmEstimate =
    estimate("gds.pageRank", graphName, config.toMap)

  // -------------------------------------------------------------------------
  // Betweenness centrality
  // -------------------------------------------------------------------------

  /** Streams betweenness scores without persisting anything. */
  def betweennessStream(
      graphName: String,
      config: BetweennessConfig = BetweennessConfig(),
      limit: Int = 0
  ): Vector[CentralityScore] = {
    logger.info(s"Betweenness stream on $graphName: ${config.describe}")
    streamScores("gds.betweenness", graphName, config.toMap, Seq.empty, limit)
  }

  def betweennessStats(graphName: String, config: BetweennessConfig = BetweennessConfig()): CentralityRunSummary =
    runNonStreaming(
      algorithm = "gds.betweenness",
      mode = "stats",
      graphName = graphName,
      propertyName = "",
      configuration = config.toMap,
      sourceLocationIds = Seq.empty
    )

  def betweennessMutate(
      graphName: String,
      config: BetweennessConfig = BetweennessConfig(),
      mutateProperty: String = DefaultBetweennessProperty
  ): CentralityRunSummary = {
    val property = CypherIdentifier.require("mutate property", mutateProperty)
    runNonStreaming(
      algorithm = "gds.betweenness",
      mode = "mutate",
      graphName = graphName,
      propertyName = property,
      configuration = config.toMap + ("mutateProperty" -> property),
      sourceLocationIds = Seq.empty
    )
  }

  def betweennessWrite(
      graphName: String,
      config: BetweennessConfig = BetweennessConfig(),
      writeProperty: String = DefaultBetweennessProperty,
      writeConcurrency: Int = 4
  ): CentralityRunSummary = {
    val property = CypherIdentifier.require("write property", writeProperty)
    runNonStreaming(
      algorithm = "gds.betweenness",
      mode = "write",
      graphName = graphName,
      propertyName = property,
      configuration = config.toMap ++ Map("writeProperty" -> property, "writeConcurrency" -> writeConcurrency),
      sourceLocationIds = Seq.empty
    )
  }

  def betweennessEstimate(graphName: String, config: BetweennessConfig = BetweennessConfig()): AlgorithmEstimate =
    estimate("gds.betweenness", graphName, config.toMap)

  // -------------------------------------------------------------------------
  // Composite importance
  // -------------------------------------------------------------------------

  /**
   * Runs both algorithms in mutate mode, reads the two property arrays back in
   * a single pass, and combines them into a ranked importance score.
   *
   * Mutate rather than stream is deliberate: two streams would require joining
   * on node identity in Scala, whereas mutating into the projection lets the
   * database do the join and returns one row per zone.
   */
  def zoneImportance(
      graphName: String,
      pageRankConfig: PageRankConfig = PageRankConfig(),
      betweennessConfig: BetweennessConfig = BetweennessConfig(),
      weights: ImportanceWeights = ImportanceWeights.Default,
      pageRankProperty: String = DefaultPageRankProperty,
      betweennessProperty: String = DefaultBetweennessProperty
  ): Vector[ZoneImportance] = {
    val prProperty = CypherIdentifier.require("page rank property", pageRankProperty)
    val bcProperty = CypherIdentifier.require("betweenness property", betweennessProperty)

    val pageRankSummary = pageRankMutate(graphName, pageRankConfig, prProperty)
    if (!pageRankSummary.didConverge) {
      logger.warn(
        s"PageRank did not converge on $graphName after ${pageRankSummary.ranIterations} iterations; " +
          "scores are the last iterate and should be treated as approximate"
      )
    }

    val betweennessSummary = betweennessMutate(graphName, betweennessConfig, bcProperty)
    logger.info(s"Centrality passes complete: ${pageRankSummary.summary} | ${betweennessSummary.summary}")

    val raw = readMutatedScores(graphName, prProperty, bcProperty)
    if (raw.isEmpty) {
      logger.warn(s"No centrality scores returned for $graphName; the projection may be empty")
      Vector.empty
    } else {
      rank(raw, weights)
    }
  }

  /**
   * Persists composite importance onto the stored zones. Zones are matched,
   * never merged: analytics output must not create nodes the ingestion path
   * has never seen.
   */
  def persistImportance(scores: Seq[ZoneImportance], batchSize: Int = 500): Long = {
    if (scores.isEmpty) {
      logger.warn("No importance scores to persist")
      0L
    } else {
      val outcome = connector.writeBatch(
        PersistImportanceCypher,
        scores.iterator.map(_.toRow),
        batchSize
      )
      logger.info(s"Persisted importance for ${scores.size} zones: ${outcome.summary}")
      outcome.propertiesSet
    }
  }

  /**
   * Convenience path: run both algorithms over a projection, rank the zones,
   * and persist the result in one call.
   */
  def analyseAndPersist(
      projection: ProjectionSummary,
      pageRankConfig: PageRankConfig = PageRankConfig(),
      betweennessConfig: BetweennessConfig = BetweennessConfig(),
      weights: ImportanceWeights = ImportanceWeights.Default
  ): Vector[ZoneImportance] = {
    if (projection.isEmpty) {
      throw new CentralityException(s"Projection ${projection.graphName} contains no nodes")
    }

    val ranked = zoneImportance(projection.graphName, pageRankConfig, betweennessConfig, weights)
    val _ = persistImportance(ranked)
    ranked
  }

  /** Zones whose corridor role exceeds their demand pull by `threshold`. */
  def corridorZones(scores: Seq[ZoneImportance], threshold: Double = 0.1): Vector[ZoneImportance] =
    scores.filter(_.corridorBias >= threshold).sortBy(-_.corridorBias).toVector

  // -------------------------------------------------------------------------
  // Ranking and normalisation
  // -------------------------------------------------------------------------

  /**
   * Min-max normalises each signal onto [0,1] before combining them. The two
   * algorithms produce scores on incomparable scales — PageRank sums to a
   * constant across the graph, betweenness grows with path count — so a raw
   * weighted sum would be dominated by whichever happens to be larger.
   */
  private[graph] def rank(raw: Vector[(Int, String, String, Double, Double)], weights: ImportanceWeights): Vector[ZoneImportance] = {
    val pageRankValues = raw.map { case (_, _, _, pageRank, _) => pageRank }
    val betweennessValues = raw.map { case (_, _, _, _, betweenness) => betweenness }

    val normalisePageRank = minMaxNormaliser(pageRankValues)
    val normaliseBetweenness = minMaxNormaliser(betweennessValues)

    raw
      .map { case (locationId, zoneName, borough, pageRank, betweenness) =>
        val normalisedPageRank = normalisePageRank(pageRank)
        val normalisedBetweenness = normaliseBetweenness(betweenness)
        val composite =
          weights.normalisedPageRank * normalisedPageRank +
            weights.normalisedBetweenness * normalisedBetweenness

        ZoneImportance(
          locationId = locationId,
          zoneName = zoneName,
          borough = borough,
          pageRank = pageRank,
          betweenness = betweenness,
          normalisedPageRank = normalisedPageRank,
          normalisedBetweenness = normalisedBetweenness,
          importanceScore = composite,
          importanceRank = 0
        )
      }
      .sortBy(entry => (-entry.importanceScore, entry.locationId))
      .zipWithIndex
      .map { case (entry, index) => entry.copy(importanceRank = index + 1) }
  }

  /**
   * Returns a normaliser for the given sample. A degenerate range — every zone
   * scoring identically, which happens on a projection with no relationships —
   * maps everything to zero rather than dividing by zero.
   */
  private def minMaxNormaliser(values: Seq[Double]): Double => Double = {
    if (values.isEmpty) { _ => 0.0 }
    else {
      val minimum = values.min
      val maximum = values.max
      val range = maximum - minimum
      if (range <= NormalisationEpsilon) { _ => 0.0 }
      else { value => (value - minimum) / range }
    }
  }

  // -------------------------------------------------------------------------
  // Procedure invocation
  // -------------------------------------------------------------------------

  /**
   * Streams scores and resolves each internal node id back to its zone.
   *
   * A personalised run needs the seed nodes as node references rather than
   * identifiers, so the seed set is matched first and merged into the config
   * map through a map projection.
   */
  private def streamScores(
      algorithm: String,
      graphName: String,
      configuration: Map[String, Any],
      sourceLocationIds: Seq[Int],
      limit: Int
  ): Vector[CentralityScore] = {
    val limitClause = if (limit > 0) s"\nLIMIT ${'$'}limit" else ""

    val cypher =
      if (sourceLocationIds.isEmpty) {
        s"""
           |CALL $algorithm.stream(${'$'}graphName, ${'$'}configuration)
           |YIELD nodeId, score
           |WITH gds.util.asNode(nodeId) AS zone, score
           |RETURN zone.locationId AS locationId,
           |       coalesce(zone.zoneName, '') AS zoneName,
           |       coalesce(zone.borough, '') AS borough,
           |       score
           |ORDER BY score DESC, locationId ASC$limitClause
           |""".stripMargin
      } else {
        s"""
           |MATCH (seed:Location)
           |WHERE seed.locationId IN ${'$'}sourceLocationIds
           |WITH collect(seed) AS sourceNodes
           |CALL $algorithm.stream(${'$'}graphName, ${'$'}configuration {.*, sourceNodes: sourceNodes})
           |YIELD nodeId, score
           |WITH gds.util.asNode(nodeId) AS zone, score
           |RETURN zone.locationId AS locationId,
           |       coalesce(zone.zoneName, '') AS zoneName,
           |       coalesce(zone.borough, '') AS borough,
           |       score
           |ORDER BY score DESC, locationId ASC$limitClause
           |""".stripMargin
      }

    val parameters = Map[String, Any](
      "graphName" -> graphName,
      "configuration" -> configuration
    ) ++
      (if (sourceLocationIds.isEmpty) Map.empty[String, Any] else Map("sourceLocationIds" -> sourceLocationIds)) ++
      (if (limit > 0) Map[String, Any]("limit" -> limit) else Map.empty[String, Any])

    connector.read(cypher, parameters) { record =>
      CentralityScore(
        locationId = record.get("locationId").asInt(0),
        zoneName = record.get("zoneName").asString(""),
        borough = record.get("borough").asString(""),
        score = record.get("score").asDouble(0.0)
      )
    }
  }

  /**
   * Invokes a stats, mutate or write run. Only PageRank reports iteration and
   * convergence data, so those fields are yielded conditionally.
   */
  private def runNonStreaming(
      algorithm: String,
      mode: String,
      graphName: String,
      propertyName: String,
      configuration: Map[String, Any],
      sourceLocationIds: Seq[Int]
  ): CentralityRunSummary = {
    val reportsIterations = algorithm == "gds.pageRank"

    val yieldFields = Seq(
      if (mode == "stats") None else Some("nodePropertiesWritten"),
      if (reportsIterations) Some("ranIterations") else None,
      if (reportsIterations) Some("didConverge") else None,
      Some("centralityDistribution"),
      Some("computeMillis"),
      if (mode == "write") Some("writeMillis") else None,
      if (mode == "mutate") Some("mutateMillis") else None,
      Some("preProcessingMillis"),
      Some("postProcessingMillis")
    ).flatten

    val yieldClause = yieldFields.mkString(", ")

    val callClause =
      if (sourceLocationIds.isEmpty) {
        s"CALL $algorithm.$mode(${'$'}graphName, ${'$'}configuration)"
      } else {
        s"CALL $algorithm.$mode(${'$'}graphName, ${'$'}configuration {.*, sourceNodes: sourceNodes})"
      }

    val prefix =
      if (sourceLocationIds.isEmpty) ""
      else
        s"""MATCH (seed:Location)
           |WHERE seed.locationId IN ${'$'}sourceLocationIds
           |WITH collect(seed) AS sourceNodes
           |""".stripMargin

    val cypher =
      s"""
         |$prefix$callClause
         |YIELD $yieldClause
         |RETURN $yieldClause
         |""".stripMargin

    val parameters = Map[String, Any](
      "graphName" -> graphName,
      "configuration" -> configuration
    ) ++ (if (sourceLocationIds.isEmpty) Map.empty[String, Any] else Map("sourceLocationIds" -> sourceLocationIds))

    logger.info(s"Running $algorithm.$mode on $graphName")

    val results = runReturning(cypher, parameters) { record =>
      val preProcessing = record.get("preProcessingMillis").asLong(0L)
      val compute = record.get("computeMillis").asLong(0L)
      val postProcessing = record.get("postProcessingMillis").asLong(0L)
      val sinkMillis =
        if (mode == "write") record.get("writeMillis").asLong(0L)
        else if (mode == "mutate") record.get("mutateMillis").asLong(0L)
        else 0L

      CentralityRunSummary(
        algorithm = algorithm,
        graphName = graphName,
        propertyName = propertyName,
        nodePropertiesWritten = if (mode == "stats") 0L else record.get("nodePropertiesWritten").asLong(0L),
        ranIterations = if (reportsIterations) record.get("ranIterations").asLong(0L) else 0L,
        didConverge = if (reportsIterations) record.get("didConverge").asBoolean(true) else true,
        distribution = CentralityDistribution.fromValue(record.get("centralityDistribution")),
        computeMillis = compute,
        totalMillis = preProcessing + compute + postProcessing + sinkMillis
      )
    }

    results.headOption match {
      case Some(summary) =>
        logger.info(s"${summary.summary} | ${summary.distribution.summary}")
        summary
      case None =>
        throw new CentralityException(s"$algorithm.$mode on $graphName returned no result row")
    }
  }

  /**
   * Reads both mutated property arrays in one pass and pivots them into a row
   * per zone. Aggregating with CASE avoids a dependency on APOC map helpers.
   */
  private def readMutatedScores(
      graphName: String,
      pageRankProperty: String,
      betweennessProperty: String
  ): Vector[(Int, String, String, Double, Double)] = {
    val cypher =
      """
        |CALL gds.graph.nodeProperties.stream($graphName, $properties)
        |YIELD nodeId, nodeProperty, propertyValue
        |WITH nodeId,
        |     sum(CASE WHEN nodeProperty = $pageRankProperty THEN toFloat(propertyValue) ELSE 0.0 END) AS pageRank,
        |     sum(CASE WHEN nodeProperty = $betweennessProperty THEN toFloat(propertyValue) ELSE 0.0 END) AS betweenness
        |WITH gds.util.asNode(nodeId) AS zone, pageRank, betweenness
        |RETURN zone.locationId AS locationId,
        |       coalesce(zone.zoneName, '') AS zoneName,
        |       coalesce(zone.borough, '') AS borough,
        |       pageRank,
        |       betweenness
        |ORDER BY locationId ASC
        |""".stripMargin

    connector.read(
      cypher,
      Map(
        "graphName" -> graphName,
        "properties" -> Seq(pageRankProperty, betweennessProperty),
        "pageRankProperty" -> pageRankProperty,
        "betweennessProperty" -> betweennessProperty
      )
    ) { record =>
      (
        record.get("locationId").asInt(0),
        record.get("zoneName").asString(""),
        record.get("borough").asString(""),
        record.get("pageRank").asDouble(0.0),
        record.get("betweenness").asDouble(0.0)
      )
    }
  }

  private def estimate(algorithm: String, graphName: String, configuration: Map[String, Any]): AlgorithmEstimate = {
    val cypher =
      s"""
         |CALL $algorithm.stream.estimate(${'$'}graphName, ${'$'}configuration)
         |YIELD requiredMemory, bytesMin, bytesMax
         |RETURN requiredMemory, bytesMin, bytesMax
         |""".stripMargin

    connector
      .readOne(cypher, Map("graphName" -> graphName, "configuration" -> configuration)) { record =>
        AlgorithmEstimate(
          algorithm = algorithm,
          requiredMemory = record.get("requiredMemory").asString("unknown"),
          bytesMin = record.get("bytesMin").asLong(0L),
          bytesMax = record.get("bytesMax").asLong(0L)
        )
      }
      .getOrElse(throw new CentralityException(s"Estimate for $algorithm on $graphName returned no result"))
  }

  /**
   * Runs a statement that both mutates the GDS catalogue and returns rows.
   * The connector's plain write path discards records, so the transaction is
   * driven directly here.
   */
  private def runReturning[T](cypher: String, parameters: Map[String, Any])(mapper: Record => T): Vector[T] = {
    val encoded = ParameterEncoder.encodeMap(parameters)

    val records: JList[Record] = connector.writeTransaction { tx: TransactionContext =>
      tx.run(cypher, encoded).list()
    }

    records.asScala.toVector.map(mapper)
  }
}

object CentralityAnalytics {

  val DefaultPageRankProperty = "pageRankScore"
  val DefaultBetweennessProperty = "betweennessScore"

  /** Range below which min-max normalisation is treated as degenerate. */
  private val NormalisationEpsilon = 1e-12

  private val PersistImportanceCypher: String =
    """
      |UNWIND $rows AS row
      |MATCH (zone:Location {locationId: row.locationId})
      |SET zone.pageRankScore           = row.pageRank,
      |    zone.betweennessScore        = row.betweenness,
      |    zone.pageRankNormalised      = row.normalisedPageRank,
      |    zone.betweennessNormalised   = row.normalisedBetweenness,
      |    zone.importanceScore         = row.importanceScore,
      |    zone.importanceRank          = row.importanceRank,
      |    zone.corridorBias            = row.corridorBias
      |""".stripMargin

  def apply(connector: Neo4jConnector): CentralityAnalytics = new CentralityAnalytics(connector)

  /**
   * Presets for the two passes, matched to the projections declared in
   * GraphProjectionBuilder.Specs.
   */
  object Presets {

    /** Trip-count weighted PageRank over the natural-orientation flow graph. */
    val demandPull: PageRankConfig = PageRankConfig(
      maxIterations = 30,
      dampingFactor = 0.85,
      tolerance = 0.0000001,
      relationshipWeightProperty = Some("tripCount")
    )

    /** Unweighted PageRank, for comparing topology against volume. */
    val topologyOnly: PageRankConfig = PageRankConfig(
      maxIterations = 20,
      relationshipWeightProperty = None
    )

    /**
     * Exact betweenness. The TLC zone graph has 265 nodes, so the exact
     * computation is cheap; sampling is only warranted if the projection is
     * widened beyond the zone dimension.
     */
    val exactCorridors: BetweennessConfig = BetweennessConfig(
      samplingSize = None,
      relationshipWeightProperty = None
    )

    /**
     * Travel-time weighted betweenness over the travel-time projection, where
     * a corridor is defined by how fast trips move through it rather than how
     * many hops it saves.
     */
    val travelTimeCorridors: BetweennessConfig = BetweennessConfig(
      samplingSize = None,
      relationshipWeightProperty = Some("avgDurationSeconds")
    )

    /** Deterministic sampled betweenness for large or dense projections. */
    def sampledCorridors(samplingSize: Int, seed: Long): BetweennessConfig = BetweennessConfig(
      samplingSize = Some(samplingSize),
      samplingSeed = Some(seed),
      relationshipWeightProperty = None
    )
  }
}
