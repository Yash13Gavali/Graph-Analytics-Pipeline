package com.taxi.analytics.graph

import java.util.function.{Function => JFunction}
import java.util.{List => JList}

import scala.collection.JavaConverters._

import com.taxi.analytics.database.{Neo4jConnector, ParameterEncoder}
import com.typesafe.scalalogging.StrictLogging
import org.neo4j.driver.{Record, TransactionContext, Value}

// ---------------------------------------------------------------------------
// Real-Time Graph Analytics & NYC Taxi Demand Forecasting
// Louvain community detection over projected zone graphs
//
// Louvain partitions the flow graph by maximising modularity: zones land in
// the same community when trips circulate between them far more than the
// degree distribution alone would predict. The result is a data-derived
// catchment map that usually cuts across administrative borough boundaries —
// which is exactly why it is worth computing rather than assuming.
//
// Two production concerns shape this module:
//
//   Orientation — Louvain requires an undirected projection. Running it
//                 against a directed graph fails inside GDS, so callers
//                 should use GraphProjectionBuilder.Specs.zoneCommunity or
//                 another spec with Orientation.Undirected.
//
//   Identity    — community identifiers are arbitrary and unstable across
//                 runs. A refresh will happily renumber every community even
//                 when the partition is unchanged, which makes naive
//                 comparisons meaningless. Seeded runs and the Rand index in
//                 `stability` exist to handle that.
// ---------------------------------------------------------------------------

/** Tuning for the Louvain pass. */
final case class LouvainConfig(
    maxLevels: Int = 10,
    maxIterations: Int = 10,
    tolerance: Double = 0.0001,
    includeIntermediateCommunities: Boolean = false,
    relationshipWeightProperty: Option[String] = Some("tripCount"),
    seedProperty: Option[String] = None,
    consecutiveIds: Boolean = false,
    minCommunitySize: Option[Int] = None,
    concurrency: Int = 4
) extends Serializable {

  require(maxLevels >= 1, "maxLevels must be at least 1")
  require(maxIterations >= 1, "maxIterations must be at least 1")
  require(tolerance > 0.0, "tolerance must be positive")
  require(concurrency >= 1, "concurrency must be at least 1")
  require(minCommunitySize.forall(_ >= 1), "minCommunitySize must be at least 1 when set")

  // GDS rejects this combination: consecutive renumbering would discard the
  // very identifiers the seed is supplying.
  require(
    !(consecutiveIds && seedProperty.isDefined),
    "consecutiveIds cannot be combined with seedProperty"
  )

  relationshipWeightProperty.foreach(property =>
    CypherIdentifier.require("relationship weight property", property)
  )
  seedProperty.foreach(property => CypherIdentifier.require("seed property", property))

  /** True when the run carries community identity forward from a prior pass. */
  def isSeeded: Boolean = seedProperty.isDefined

  private[graph] def toMap: Map[String, Any] = {
    val base = Map[String, Any](
      "maxLevels" -> maxLevels,
      "maxIterations" -> maxIterations,
      "tolerance" -> tolerance,
      "includeIntermediateCommunities" -> includeIntermediateCommunities,
      "consecutiveIds" -> consecutiveIds,
      "concurrency" -> concurrency
    )
    val weighted = relationshipWeightProperty.fold(base)(property => base + ("relationshipWeightProperty" -> property))
    val seeded = seedProperty.fold(weighted)(property => weighted + ("seedProperty" -> property))
    minCommunitySize.fold(seeded)(size => seeded + ("minCommunitySize" -> size))
  }

  def describe: String =
    s"levels=$maxLevels iterations=$maxIterations tolerance=$tolerance " +
      s"weight=${relationshipWeightProperty.getOrElse("none")} seeded=$isSeeded " +
      s"hierarchical=$includeIntermediateCommunities"
}

/** One zone's community membership. */
final case class CommunityAssignment(
    locationId: Int,
    zoneName: String,
    borough: String,
    communityId: Long,
    intermediateCommunityIds: Seq[Long]
) {

  /** Depth of the dendrogram this assignment was resolved at. */
  def levels: Int = intermediateCommunityIds.size

  private[graph] def toRow: Map[String, Any] = Map(
    "locationId" -> locationId,
    "communityId" -> communityId,
    "levels" -> levels
  )
}

/** Percentile summary of community sizes returned by GDS. */
final case class CommunitySizeDistribution(
    min: Double,
    mean: Double,
    max: Double,
    p50: Double,
    p75: Double,
    p90: Double,
    p95: Double,
    p99: Double,
    p999: Double
) {
  def summary: String =
    f"min=$min%.1f p50=$p50%.1f p90=$p90%.1f p99=$p99%.1f max=$max%.1f mean=$mean%.2f"
}

object CommunitySizeDistribution {

  val Empty: CommunitySizeDistribution =
    CommunitySizeDistribution(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

  private[graph] def fromValue(value: Value): CommunitySizeDistribution =
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

      CommunitySizeDistribution(
        min = at("min"),
        mean = at("mean"),
        max = at("max"),
        p50 = at("p50"),
        p75 = at("p75"),
        p90 = at("p90"),
        p95 = at("p95"),
        p99 = at("p99"),
        p999 = at("p999")
      )
    }
}

/** Outcome of a non-streaming Louvain run. */
final case class LouvainRunSummary(
    graphName: String,
    propertyName: String,
    communityCount: Long,
    modularity: Double,
    modularities: Seq[Double],
    ranLevels: Long,
    nodePropertiesWritten: Long,
    distribution: CommunitySizeDistribution,
    computeMillis: Long,
    totalMillis: Long
) {

  /**
   * Modularity below roughly 0.3 means the partition barely beats a random
   * assignment; above 0.7 usually means the graph is close to disconnected.
   */
  def hasMeaningfulStructure: Boolean = modularity >= 0.3

  def summary: String =
    f"graph=$graphName communities=$communityCount%d modularity=$modularity%.4f " +
      f"levels=$ranLevels%d nodes=$nodePropertiesWritten%d compute_ms=$computeMillis%d total_ms=$totalMillis%d"
}

/** Aggregate profile of one detected community. */
final case class CommunityProfile(
    communityId: Long,
    size: Int,
    locationIds: Seq[Int],
    topZones: Seq[String],
    dominantBorough: String,
    dominantBoroughShare: Double,
    boroughSpread: Int,
    internalTripCount: Long,
    externalTripCount: Long
) {

  /**
   * Share of the community's outbound volume that stays inside it. High
   * cohesion marks a self-contained catchment; low cohesion marks a community
   * the partition split for topological reasons that trip volume does not
   * support.
   */
  def cohesion: Double = {
    val total = internalTripCount + externalTripCount
    if (total <= 0L) 0.0 else internalTripCount.toDouble / total.toDouble
  }

  /** True when the community ignores borough boundaries entirely. */
  def crossesBoroughs: Boolean = boroughSpread > 1

  private[graph] def toRow: Map[String, Any] = Map(
    "communityId" -> communityId,
    "size" -> size,
    "dominantBorough" -> dominantBorough,
    "dominantBoroughShare" -> dominantBoroughShare,
    "boroughSpread" -> boroughSpread,
    "internalTripCount" -> internalTripCount,
    "externalTripCount" -> externalTripCount,
    "cohesion" -> cohesion
  )

  def summary: String =
    f"community=$communityId%d size=$size%d cohesion=${cohesion}%.3f " +
      f"borough=$dominantBorough share=${dominantBoroughShare}%.2f spread=$boroughSpread%d"
}

/** Agreement between two partitions of the same zone set. */
final case class ClusterStability(
    sharedZones: Int,
    agreeingPairs: Long,
    disagreeingPairs: Long,
    randIndex: Double
) {

  /** Below this the partition has churned enough that downstream labels are stale. */
  def isStable: Boolean = randIndex >= 0.9

  def summary: String =
    f"shared_zones=$sharedZones%d rand_index=$randIndex%.4f " +
      f"agree=$agreeingPairs%d disagree=$disagreeingPairs%d"
}

/** Raised when a clustering run does not return the expected result. */
final class ClusterException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

/**
 * Executes Louvain against projections held in the GDS catalogue and turns the
 * raw partition into something the rest of the system can use: persisted
 * membership, per-community profiles, and a stability measure against the
 * previous partition.
 *
 * Like the centrality pass, nothing here creates or drops a projection.
 */
final class ClusterAnalytics(connector: Neo4jConnector) extends StrictLogging {

  import ClusterAnalytics._

  // -------------------------------------------------------------------------
  // Louvain execution modes
  // -------------------------------------------------------------------------

  /** Streams community membership without persisting anything. */
  def louvainStream(
      graphName: String,
      config: LouvainConfig = LouvainConfig()
  ): Vector[CommunityAssignment] = {
    logger.info(s"Louvain stream on $graphName: ${config.describe}")

    val intermediateProjection =
      if (config.includeIntermediateCommunities)
        "coalesce(intermediateCommunityIds, []) AS intermediateCommunityIds"
      else "[] AS intermediateCommunityIds"

    val yieldClause =
      if (config.includeIntermediateCommunities) "nodeId, communityId, intermediateCommunityIds"
      else "nodeId, communityId"

    val cypher =
      s"""
         |CALL gds.louvain.stream(${'$'}graphName, ${'$'}configuration)
         |YIELD $yieldClause
         |WITH gds.util.asNode(nodeId) AS zone, communityId, $intermediateProjection
         |RETURN zone.locationId AS locationId,
         |       coalesce(zone.zoneName, '') AS zoneName,
         |       coalesce(zone.borough, '') AS borough,
         |       communityId,
         |       intermediateCommunityIds
         |ORDER BY communityId ASC, locationId ASC
         |""".stripMargin

    guardUndirected(graphName) {
      connector.read(cypher, Map("graphName" -> graphName, "configuration" -> config.toMap)) { record =>
        CommunityAssignment(
          locationId = record.get("locationId").asInt(0),
          zoneName = record.get("zoneName").asString(""),
          borough = record.get("borough").asString(""),
          communityId = record.get("communityId").asLong(0L),
          intermediateCommunityIds = toLongSeq(record.get("intermediateCommunityIds"))
        )
      }
    }
  }

  /** Runs Louvain for its modularity and size distribution only. */
  def louvainStats(graphName: String, config: LouvainConfig = LouvainConfig()): LouvainRunSummary =
    runNonStreaming("stats", graphName, "", config)

  /** Writes community membership into the in-memory graph for a later pass. */
  def louvainMutate(
      graphName: String,
      config: LouvainConfig = LouvainConfig(),
      mutateProperty: String = DefaultCommunityProperty
  ): LouvainRunSummary = {
    val property = CypherIdentifier.require("mutate property", mutateProperty)
    runNonStreaming("mutate", graphName, property, config, Map("mutateProperty" -> property))
  }

  /** Persists community membership onto the stored `:Location` nodes. */
  def louvainWrite(
      graphName: String,
      config: LouvainConfig = LouvainConfig(),
      writeProperty: String = DefaultCommunityProperty,
      writeConcurrency: Int = 4
  ): LouvainRunSummary = {
    val property = CypherIdentifier.require("write property", writeProperty)
    runNonStreaming(
      "write",
      graphName,
      property,
      config,
      Map("writeProperty" -> property, "writeConcurrency" -> writeConcurrency)
    )
  }

  def louvainEstimate(graphName: String, config: LouvainConfig = LouvainConfig()): AlgorithmEstimate = {
    val cypher =
      """
        |CALL gds.louvain.stream.estimate($graphName, $configuration)
        |YIELD requiredMemory, bytesMin, bytesMax
        |RETURN requiredMemory, bytesMin, bytesMax
        |""".stripMargin

    connector
      .readOne(cypher, Map("graphName" -> graphName, "configuration" -> config.toMap)) { record =>
        AlgorithmEstimate(
          algorithm = "gds.louvain",
          requiredMemory = record.get("requiredMemory").asString("unknown"),
          bytesMin = record.get("bytesMin").asLong(0L),
          bytesMax = record.get("bytesMax").asLong(0L)
        )
      }
      .getOrElse(throw new ClusterException(s"Estimate for gds.louvain on $graphName returned no result"))
  }

  // -------------------------------------------------------------------------
  // Persistence
  // -------------------------------------------------------------------------

  /**
   * Persists membership onto the zones and materialises a `:ZoneCommunity`
   * node per community.
   *
   * Zones are matched rather than merged, and a membership edge that no longer
   * agrees with the new assignment is deleted in the same statement — without
   * that, a zone which moves between communities accumulates stale edges and
   * every downstream community traversal double-counts it.
   */
  def persistCommunities(
      assignments: Seq[CommunityAssignment],
      batchSize: Int = 500
  ): Long = {
    if (assignments.isEmpty) {
      logger.warn("No community assignments to persist")
      0L
    } else {
      val outcome = connector.writeBatch(
        PersistMembershipCypher,
        assignments.iterator.map(_.toRow),
        batchSize
      )
      logger.info(s"Persisted membership for ${assignments.size} zones: ${outcome.summary}")
      outcome.propertiesSet
    }
  }

  /** Writes aggregate profile statistics onto the `:ZoneCommunity` nodes. */
  def persistProfiles(profiles: Seq[CommunityProfile], batchSize: Int = 200): Long = {
    if (profiles.isEmpty) {
      logger.warn("No community profiles to persist")
      0L
    } else {
      val outcome = connector.writeBatch(
        PersistProfileCypher,
        profiles.iterator.map(_.toRow),
        batchSize
      )
      logger.info(s"Persisted ${profiles.size} community profiles: ${outcome.summary}")
      outcome.propertiesSet
    }
  }

  /**
   * Removes `:ZoneCommunity` nodes that no zone belongs to any more. A run
   * that merges two communities leaves the vacated identifier behind.
   */
  def pruneEmptyCommunities(): Long = {
    val outcome = connector.write(
      """
        |MATCH (community:ZoneCommunity)
        |WHERE NOT (community)<-[:MEMBER_OF]-(:Location)
        |DETACH DELETE community
        |""".stripMargin
    )
    if (outcome.nodesDeleted > 0L) {
      logger.info(s"Pruned ${outcome.nodesDeleted} empty communities")
    }
    outcome.nodesDeleted
  }

  // -------------------------------------------------------------------------
  // Profiling
  // -------------------------------------------------------------------------

  /**
   * Builds a profile per community from the persisted membership. Call after
   * [[persistCommunities]]: the profile reads `communityId` from the store so
   * that flow volume can be attributed without shipping the whole partition
   * back into the query as a parameter.
   *
   * `windowFrom` and `windowTo` bound the flow edges the same way the
   * projection did; leaving them unset attributes the full retained history.
   */
  def profileCommunities(
      windowFrom: Option[Long] = None,
      windowTo: Option[Long] = None,
      topZoneCount: Int = 5
  ): Vector[CommunityProfile] = {
    require(topZoneCount >= 1, "topZoneCount must be at least 1")

    val membership = readMembership(topZoneCount)
    if (membership.isEmpty) {
      logger.warn("No persisted community membership found; run persistCommunities first")
      Vector.empty
    } else {
      val volume = readCommunityVolume(windowFrom, windowTo)

      membership.map { entry =>
        val (internal, external) = volume.getOrElse(entry.communityId, (0L, 0L))
        entry.profile.copy(internalTripCount = internal, externalTripCount = external)
      }.sortBy(profile => (-profile.size, profile.communityId))
    }
  }

  /** Communities whose members span more than one borough, widest first. */
  def crossBoroughCommunities(profiles: Seq[CommunityProfile]): Vector[CommunityProfile] =
    profiles.filter(_.crossesBoroughs).sortBy(profile => (-profile.boroughSpread, -profile.size)).toVector

  /** Communities whose trips mostly leave them, weakest cohesion first. */
  def weaklyCohesiveCommunities(profiles: Seq[CommunityProfile], threshold: Double = 0.5): Vector[CommunityProfile] =
    profiles.filter(profile => profile.cohesion < threshold).sortBy(_.cohesion).toVector

  // -------------------------------------------------------------------------
  // Stability
  // -------------------------------------------------------------------------

  /**
   * Rand index between two partitions of the same zone set.
   *
   * Community identifiers are arbitrary, so comparing them directly says
   * nothing. The Rand index instead counts zone pairs that both partitions
   * agree about — either grouped together in both, or separated in both — and
   * is therefore invariant to relabelling. On the 265-zone TLC graph the
   * pairwise comparison is trivially cheap.
   */
  def stability(previous: Seq[CommunityAssignment], current: Seq[CommunityAssignment]): ClusterStability = {
    val previousById = previous.map(assignment => assignment.locationId -> assignment.communityId).toMap
    val currentById = current.map(assignment => assignment.locationId -> assignment.communityId).toMap

    val shared = previousById.keySet.intersect(currentById.keySet).toVector.sorted

    if (shared.size < 2) {
      ClusterStability(shared.size, 0L, 0L, if (shared.isEmpty) 0.0 else 1.0)
    } else {
      var agreeing = 0L
      var disagreeing = 0L
      var i = 0

      while (i < shared.size) {
        var j = i + 1
        while (j < shared.size) {
          val left = shared(i)
          val right = shared(j)
          val togetherBefore = previousById(left) == previousById(right)
          val togetherNow = currentById(left) == currentById(right)
          if (togetherBefore == togetherNow) agreeing += 1L else disagreeing += 1L
          j += 1
        }
        i += 1
      }

      val total = agreeing + disagreeing
      ClusterStability(
        sharedZones = shared.size,
        agreeingPairs = agreeing,
        disagreeingPairs = disagreeing,
        randIndex = if (total == 0L) 1.0 else agreeing.toDouble / total.toDouble
      )
    }
  }

  /** Membership currently persisted on the zones, for comparison against a new run. */
  def persistedAssignments(): Vector[CommunityAssignment] =
    connector.read(
      """
        |MATCH (zone:Location)
        |WHERE zone.communityId IS NOT NULL
        |RETURN zone.locationId AS locationId,
        |       coalesce(zone.zoneName, '') AS zoneName,
        |       coalesce(zone.borough, '') AS borough,
        |       zone.communityId AS communityId
        |ORDER BY locationId ASC
        |""".stripMargin
    ) { record =>
      CommunityAssignment(
        locationId = record.get("locationId").asInt(0),
        zoneName = record.get("zoneName").asString(""),
        borough = record.get("borough").asString(""),
        communityId = record.get("communityId").asLong(0L),
        intermediateCommunityIds = Seq.empty
      )
    }

  // -------------------------------------------------------------------------
  // Orchestration
  // -------------------------------------------------------------------------

  /**
   * Full pass: detect, compare against the previous partition, persist, and
   * profile. Returns the assignments alongside the profiles and the stability
   * measure so a caller can decide whether the change is worth acting on.
   */
  def analyseAndPersist(
      projection: ProjectionSummary,
      config: LouvainConfig = LouvainConfig(),
      windowFrom: Option[Long] = None,
      windowTo: Option[Long] = None
  ): (Vector[CommunityAssignment], Vector[CommunityProfile], ClusterStability) = {
    if (projection.isEmpty) {
      throw new ClusterException(s"Projection ${projection.graphName} contains no nodes")
    }
    if (projection.relationshipCount == 0L) {
      throw new ClusterException(
        s"Projection ${projection.graphName} has no relationships; Louvain would return one community per zone"
      )
    }

    val statistics = louvainStats(projection.graphName, config)
    logger.info(s"${statistics.summary} | sizes ${statistics.distribution.summary}")
    if (!statistics.hasMeaningfulStructure) {
      logger.warn(
        f"Modularity ${statistics.modularity}%.4f is low; the partition is close to arbitrary. " +
          "Widen the projection window or lower the minimum trip threshold."
      )
    }

    val previous = persistedAssignments()
    val assignments = louvainStream(projection.graphName, config)

    val drift = stability(previous, assignments)
    if (previous.nonEmpty) {
      logger.info(s"Partition stability against previous run: ${drift.summary}")
      if (!drift.isStable) {
        logger.warn("Community partition changed substantially; downstream community labels are stale")
      }
    }

    val _ = persistCommunities(assignments)
    val _ = pruneEmptyCommunities()

    val profiles = profileCommunities(windowFrom, windowTo)
    val _ = persistProfiles(profiles)

    (assignments, profiles, drift)
  }

  // -------------------------------------------------------------------------
  // Procedure invocation
  // -------------------------------------------------------------------------

  private def runNonStreaming(
      mode: String,
      graphName: String,
      propertyName: String,
      config: LouvainConfig,
      extraConfiguration: Map[String, Any] = Map.empty
  ): LouvainRunSummary = {
    val yieldFields = Seq(
      if (mode == "stats") None else Some("nodePropertiesWritten"),
      Some("communityCount"),
      Some("modularity"),
      Some("modularities"),
      Some("ranLevels"),
      Some("communityDistribution"),
      Some("preProcessingMillis"),
      Some("computeMillis"),
      Some("postProcessingMillis"),
      if (mode == "write") Some("writeMillis") else None,
      if (mode == "mutate") Some("mutateMillis") else None
    ).flatten

    val yieldClause = yieldFields.mkString(", ")

    val cypher =
      s"""
         |CALL gds.louvain.$mode(${'$'}graphName, ${'$'}configuration)
         |YIELD $yieldClause
         |RETURN $yieldClause
         |""".stripMargin

    logger.info(s"Running gds.louvain.$mode on $graphName: ${config.describe}")

    val results = guardUndirected(graphName) {
      runReturning(
        cypher,
        Map("graphName" -> graphName, "configuration" -> (config.toMap ++ extraConfiguration))
      ) { record =>
        val preProcessing = record.get("preProcessingMillis").asLong(0L)
        val compute = record.get("computeMillis").asLong(0L)
        val postProcessing = record.get("postProcessingMillis").asLong(0L)
        val sinkMillis =
          if (mode == "write") record.get("writeMillis").asLong(0L)
          else if (mode == "mutate") record.get("mutateMillis").asLong(0L)
          else 0L

        LouvainRunSummary(
          graphName = graphName,
          propertyName = propertyName,
          communityCount = record.get("communityCount").asLong(0L),
          modularity = record.get("modularity").asDouble(0.0),
          modularities = toDoubleSeq(record.get("modularities")),
          ranLevels = record.get("ranLevels").asLong(0L),
          nodePropertiesWritten =
            if (mode == "stats") 0L else record.get("nodePropertiesWritten").asLong(0L),
          distribution = CommunitySizeDistribution.fromValue(record.get("communityDistribution")),
          computeMillis = compute,
          totalMillis = preProcessing + compute + postProcessing + sinkMillis
        )
      }
    }

    results.headOption match {
      case Some(summary) =>
        logger.info(summary.summary)
        summary
      case None =>
        throw new ClusterException(s"gds.louvain.$mode on $graphName returned no result row")
    }
  }

  /**
   * Membership and borough composition, read back from the store. Zones are
   * ordered by importance so that the leading names in each community are the
   * ones a reader would recognise.
   */
  private def readMembership(topZoneCount: Int): Vector[MembershipEntry] =
    connector.read(
      """
        |MATCH (zone:Location)
        |WHERE zone.communityId IS NOT NULL
        |WITH zone.communityId AS communityId, zone
        |ORDER BY coalesce(zone.importanceScore, 0.0) DESC, zone.locationId ASC
        |WITH communityId,
        |     collect(zone.locationId) AS locationIds,
        |     collect(coalesce(zone.zoneName, '')) AS zoneNames,
        |     collect(coalesce(zone.borough, '')) AS boroughs
        |RETURN communityId,
        |       locationIds,
        |       zoneNames[0..$topZoneCount] AS topZones,
        |       boroughs
        |ORDER BY size(locationIds) DESC, communityId ASC
        |""".stripMargin,
      Map("topZoneCount" -> topZoneCount)
    ) { record =>
      val locationIds = toIntSeq(record.get("locationIds"))
      val boroughs = toStringSeq(record.get("boroughs")).filter(_.nonEmpty)

      val boroughCounts = boroughs.groupBy(identity).map { case (name, occurrences) => name -> occurrences.size }
      val (dominantBorough, dominantCount) =
        if (boroughCounts.isEmpty) ("", 0)
        else boroughCounts.maxBy { case (name, count) => (count, name) }

      MembershipEntry(
        communityId = record.get("communityId").asLong(0L),
        profile = CommunityProfile(
          communityId = record.get("communityId").asLong(0L),
          size = locationIds.size,
          locationIds = locationIds,
          topZones = toStringSeq(record.get("topZones")),
          dominantBorough = dominantBorough,
          dominantBoroughShare =
            if (boroughs.isEmpty) 0.0 else dominantCount.toDouble / boroughs.size.toDouble,
          boroughSpread = boroughCounts.size,
          internalTripCount = 0L,
          externalTripCount = 0L
        )
      )
    }

  /** Internal and outbound trip volume per community, from the stored flows. */
  private def readCommunityVolume(
      windowFrom: Option[Long],
      windowTo: Option[Long]
  ): Map[Long, (Long, Long)] = {
    val windowPredicates = Seq(
      windowFrom.map(_ => "flow.windowStart >= $windowFrom"),
      windowTo.map(_ => "flow.windowStart < $windowTo")
    ).flatten

    val windowClause =
      if (windowPredicates.isEmpty) "" else s"\n  AND ${windowPredicates.mkString("\n  AND ")}"

    val cypher =
      s"""
         |MATCH (origin:Location)-[flow:TRIP_FLOW]->(destination:Location)
         |WHERE origin.communityId IS NOT NULL
         |  AND destination.communityId IS NOT NULL$windowClause
         |WITH origin.communityId AS communityId,
         |     sum(CASE WHEN origin.communityId = destination.communityId
         |              THEN coalesce(flow.tripCount, 0) ELSE 0 END) AS internalTrips,
         |     sum(CASE WHEN origin.communityId <> destination.communityId
         |              THEN coalesce(flow.tripCount, 0) ELSE 0 END) AS externalTrips
         |RETURN communityId, internalTrips, externalTrips
         |""".stripMargin

    val parameters =
      windowFrom.map(value => "windowFrom" -> (value: Any)).toMap ++
        windowTo.map(value => "windowTo" -> (value: Any)).toMap

    connector
      .read(cypher, parameters) { record =>
        record.get("communityId").asLong(0L) ->
          ((record.get("internalTrips").asLong(0L), record.get("externalTrips").asLong(0L)))
      }
      .toMap
  }

  /**
   * Translates the GDS orientation failure into a message that names the fix.
   * Louvain rejects directed projections, and the raw error does not mention
   * which projection spec produced them.
   */
  private def guardUndirected[T](graphName: String)(body: => T): T =
    try body
    catch {
      case error: Throwable if isOrientationFailure(error) =>
        throw new ClusterException(
          s"Louvain requires an undirected projection, but $graphName is directed. " +
            "Rebuild it with Orientation.Undirected, for example GraphProjectionBuilder.Specs.zoneCommunity.",
          error
        )
    }

  private def isOrientationFailure(error: Throwable): Boolean = {
    val message = Option(error.getMessage).getOrElse("").toLowerCase
    message.contains("undirected") || message.contains("orientation")
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

object ClusterAnalytics {

  val DefaultCommunityProperty = "communityId"

  /** Intermediate carrier joining membership to volume during profiling. */
  private final case class MembershipEntry(communityId: Long, profile: CommunityProfile)

  private val PersistMembershipCypher: String =
    """
      |UNWIND $rows AS row
      |MATCH (zone:Location {locationId: row.locationId})
      |SET zone.communityId = row.communityId,
      |    zone.communityLevels = row.levels
      |WITH row, zone
      |MERGE (community:ZoneCommunity {communityId: row.communityId})
      |MERGE (zone)-[:MEMBER_OF]->(community)
      |WITH row, zone
      |OPTIONAL MATCH (zone)-[stale:MEMBER_OF]->(other:ZoneCommunity)
      |WHERE other.communityId <> row.communityId
      |DELETE stale
      |""".stripMargin

  private val PersistProfileCypher: String =
    """
      |UNWIND $rows AS row
      |MATCH (community:ZoneCommunity {communityId: row.communityId})
      |SET community.size                 = row.size,
      |    community.dominantBorough      = row.dominantBorough,
      |    community.dominantBoroughShare = row.dominantBoroughShare,
      |    community.boroughSpread        = row.boroughSpread,
      |    community.internalTripCount    = row.internalTripCount,
      |    community.externalTripCount    = row.externalTripCount,
      |    community.cohesion             = row.cohesion
      |""".stripMargin

  def apply(connector: Neo4jConnector): ClusterAnalytics = new ClusterAnalytics(connector)

  private[graph] def toLongSeq(value: Value): Seq[Long] =
    if (value == null || value.isNull) Seq.empty
    else value.asList(new JFunction[Value, java.lang.Long] {
      override def apply(entry: Value): java.lang.Long =
        if (entry == null || entry.isNull) java.lang.Long.valueOf(0L)
        else java.lang.Long.valueOf(entry.asLong(0L))
    }).asScala.toVector.map(_.longValue())

  private[graph] def toIntSeq(value: Value): Seq[Int] =
    if (value == null || value.isNull) Seq.empty
    else value.asList(new JFunction[Value, java.lang.Integer] {
      override def apply(entry: Value): java.lang.Integer =
        if (entry == null || entry.isNull) java.lang.Integer.valueOf(0)
        else java.lang.Integer.valueOf(entry.asInt(0))
    }).asScala.toVector.map(_.intValue())

  private[graph] def toDoubleSeq(value: Value): Seq[Double] =
    if (value == null || value.isNull) Seq.empty
    else value.asList(new JFunction[Value, java.lang.Double] {
      override def apply(entry: Value): java.lang.Double =
        if (entry == null || entry.isNull) java.lang.Double.valueOf(0.0)
        else java.lang.Double.valueOf(entry.asDouble(0.0))
    }).asScala.toVector.map(_.doubleValue())

  private[graph] def toStringSeq(value: Value): Seq[String] =
    if (value == null || value.isNull) Seq.empty
    else value.asList(new JFunction[Value, String] {
      override def apply(entry: Value): String =
        if (entry == null || entry.isNull) "" else entry.asString("")
    }).asScala.toVector

  /**
   * Presets matched to the projections declared in
   * GraphProjectionBuilder.Specs. All of them assume an undirected projection.
   */
  object Presets {

    /** Volume-weighted catchment detection over the undirected flow graph. */
    val catchments: LouvainConfig = LouvainConfig(
      maxLevels = 10,
      maxIterations = 10,
      tolerance = 0.0001,
      relationshipWeightProperty = Some("tripCount")
    )

    /** Unweighted run, for comparing topology against volume-driven grouping. */
    val topologyOnly: LouvainConfig = LouvainConfig(
      relationshipWeightProperty = None
    )

    /**
     * Retains the full dendrogram so that a caller can pick a coarser or finer
     * level than the one Louvain settled on.
     */
    val hierarchical: LouvainConfig = LouvainConfig(
      includeIntermediateCommunities = true,
      relationshipWeightProperty = Some("tripCount")
    )

    /**
     * Seeded run that carries identifiers forward from the previous partition,
     * so a refresh renumbers only what genuinely moved. Requires the seed
     * property to be present as a node property on the projection, which means
     * projecting `communityId` alongside the other node properties.
     */
    def seeded(seedProperty: String = DefaultCommunityProperty): LouvainConfig = LouvainConfig(
      relationshipWeightProperty = Some("tripCount"),
      seedProperty = Some(seedProperty)
    )

    /** Suppresses singleton and near-singleton communities from the output. */
    def coarse(minCommunitySize: Int): LouvainConfig = LouvainConfig(
      relationshipWeightProperty = Some("tripCount"),
      minCommunitySize = Some(minCommunitySize)
    )
  }
}
