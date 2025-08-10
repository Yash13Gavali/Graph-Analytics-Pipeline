package com.taxi.analytics.graph

import java.util.function.{Function => JFunction}
import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.taxi.analytics.database.{Neo4jConnector, ParameterEncoder}
import com.typesafe.scalalogging.StrictLogging
import org.neo4j.driver.{Record, TransactionContext, Value}

// ---------------------------------------------------------------------------
// Real-Time Graph Analytics & NYC Taxi Demand Forecasting
// Dynamic Graph Data Science projection builder
//
// The demand graph grows continuously, so centrality and community passes run
// against a bounded slice of it: the zone-to-zone flows observed inside an
// event-time window, pre-aggregated across the individual window buckets that
// the streaming layer writes.
//
// Projections are versioned rather than mutated in place. Building generation
// n+1 before dropping generation n means readers holding the previous name
// keep working while the new slice materialises, which matters because GDS
// offers no rename and no atomic replace.
// ---------------------------------------------------------------------------

/** Direction the projected relationships are traversed in. */
sealed abstract class Orientation(val value: String) extends Serializable

object Orientation {
  case object Natural extends Orientation("NATURAL")
  case object Reverse extends Orientation("REVERSE")
  case object Undirected extends Orientation("UNDIRECTED")

  val All: Seq[Orientation] = Seq(Natural, Reverse, Undirected)

  def fromString(raw: String): Either[String, Orientation] = {
    val normalized = raw.trim.toUpperCase
    All.find(_.value == normalized).toRight(s"Unknown orientation '$raw'; expected one of ${All.map(_.value).mkString(", ")}")
  }
}

/** How parallel flow edges collapse into a single projected relationship. */
sealed abstract class PropertyAggregation(val function: String) extends Serializable

object PropertyAggregation {
  case object Sum extends PropertyAggregation("sum")
  case object Avg extends PropertyAggregation("avg")
  case object Min extends PropertyAggregation("min")
  case object Max extends PropertyAggregation("max")
  case object Count extends PropertyAggregation("count")

  val All: Seq[PropertyAggregation] = Seq(Sum, Avg, Min, Max, Count)

  def fromString(raw: String): Either[String, PropertyAggregation] = {
    val normalized = raw.trim.toLowerCase
    All.find(_.function == normalized).toRight(s"Unknown aggregation '$raw'; expected one of ${All.map(_.function).mkString(", ")}")
  }
}

/** Guards every identifier that has to be interpolated into a Cypher string. */
object CypherIdentifier {

  private val Pattern = "^[A-Za-z_][A-Za-z0-9_]*$".r

  def validate(kind: String, value: String): Either[String, String] = {
    val trimmed = value.trim
    if (Pattern.findFirstIn(trimmed).isDefined) Right(trimmed)
    else Left(s"$kind '$value' is not a valid Cypher identifier")
  }

  def require(kind: String, value: String): String =
    validate(kind, value) match {
      case Right(valid) => valid
      case Left(reason) => throw new IllegalArgumentException(reason)
    }

  /** Backtick-quotes an already validated identifier. */
  def quote(identifier: String): String = s"`$identifier`"
}

/**
 * A node property carried into the projection.
 *
 * @param name           property name inside the projected graph
 * @param sourceProperty property name on the stored node
 * @param defaultValue   substituted when the stored property is absent
 */
final case class NodeProperty(name: String, sourceProperty: String, defaultValue: Double) extends Serializable {
  val validatedName: String = CypherIdentifier.require("node property name", name)
  val validatedSource: String = CypherIdentifier.require("node source property", sourceProperty)

  private[graph] def expression(alias: String): String =
    s"${CypherIdentifier.quote(validatedName)}: coalesce($alias.${CypherIdentifier.quote(validatedSource)}, $defaultValue)"
}

/**
 * A relationship property, aggregated across every flow edge that connects the
 * same zone pair inside the projection window.
 */
final case class RelationshipProperty(
    name: String,
    sourceProperty: String,
    aggregation: PropertyAggregation,
    defaultValue: Double
) extends Serializable {

  val validatedName: String = CypherIdentifier.require("relationship property name", name)
  val validatedSource: String = CypherIdentifier.require("relationship source property", sourceProperty)

  /** Aggregation expression evaluated in the pre-projection WITH clause. */
  private[graph] def aggregationExpression(alias: String): String = {
    val quotedName = CypherIdentifier.quote(validatedName)
    aggregation match {
      case PropertyAggregation.Count =>
        s"toFloat(count($alias)) AS $quotedName"
      case other =>
        s"coalesce(${other.function}($alias.${CypherIdentifier.quote(validatedSource)}), $defaultValue) AS $quotedName"
    }
  }

  private[graph] def projectionEntry: String = {
    val quotedName = CypherIdentifier.quote(validatedName)
    s"$quotedName: toFloat(coalesce($quotedName, $defaultValue))"
  }
}

/**
 * Declarative description of a projection slice.
 *
 * `windowFrom` and `windowTo` bound the flow edges by their `windowStart`
 * property as a half-open interval in epoch milliseconds. Leaving both unset
 * projects the entire retained history.
 */
final case class ProjectionSpec(
    logicalName: String,
    nodeLabel: String = "Location",
    relationshipType: String = "TRIP_FLOW",
    windowFrom: Option[Long] = None,
    windowTo: Option[Long] = None,
    minTripCount: Long = 1L,
    includeInternalFlows: Boolean = false,
    includeIsolatedNodes: Boolean = true,
    orientation: Orientation = Orientation.Natural,
    boroughFilter: Seq[String] = Seq.empty,
    serviceZoneFilter: Seq[String] = Seq.empty,
    nodeProperties: Seq[NodeProperty] = Seq.empty,
    relationshipProperties: Seq[RelationshipProperty] = Seq.empty,
    readConcurrency: Int = 4,
    retainGenerations: Int = 1
) extends Serializable {

  val validatedLogicalName: String = CypherIdentifier.require("projection name", logicalName)
  val validatedNodeLabel: String = CypherIdentifier.require("node label", nodeLabel)
  val validatedRelationshipType: String = CypherIdentifier.require("relationship type", relationshipType)

  require(minTripCount >= 0L, "minTripCount must be non-negative")
  require(readConcurrency >= 1, "readConcurrency must be at least 1")
  require(retainGenerations >= 1, "retainGenerations must be at least 1")
  require(
    windowFrom.isEmpty || windowTo.isEmpty || windowFrom.get < windowTo.get,
    "windowFrom must be strictly less than windowTo"
  )
  require(
    relationshipProperties.map(_.validatedName).distinct.size == relationshipProperties.size,
    "relationship property names must be unique"
  )
  require(
    nodeProperties.map(_.validatedName).distinct.size == nodeProperties.size,
    "node property names must be unique"
  )

  def describe: String = {
    val window = (windowFrom, windowTo) match {
      case (Some(from), Some(to)) => s"[$from,$to)"
      case (Some(from), None) => s"[$from,)"
      case (None, Some(to)) => s"(,$to)"
      case (None, None) => "unbounded"
    }
    s"name=$validatedLogicalName label=$validatedNodeLabel type=$validatedRelationshipType " +
      s"window=$window orientation=${orientation.value} min_trips=$minTripCount"
  }
}

/** Catalogue statistics for a materialised projection. */
final case class ProjectionSummary(
    graphName: String,
    logicalName: String,
    generation: Long,
    nodeCount: Long,
    relationshipCount: Long,
    density: Double,
    sizeInBytes: Long,
    memoryUsage: String,
    degreeDistribution: Map[String, Double],
    projectMillis: Long
) {

  def isEmpty: Boolean = nodeCount == 0L

  def summary: String =
    f"graph=$graphName nodes=$nodeCount%d relationships=$relationshipCount%d " +
      f"density=$density%.6f memory=$memoryUsage project_ms=$projectMillis%d"
}

/** Result of a pre-projection memory estimate. */
final case class ProjectionEstimate(
    requiredMemory: String,
    bytesMin: Long,
    bytesMax: Long,
    nodeCount: Long,
    relationshipCount: Long
) {
  def summary: String =
    s"required=$requiredMemory bytes_min=$bytesMin bytes_max=$bytesMax " +
      s"nodes=$nodeCount relationships=$relationshipCount"
}

/** Row counts observed in the store before a projection is attempted. */
final case class SourceCardinality(nodeCount: Long, relationshipCount: Long, distinctPairs: Long) {
  def summary: String = s"nodes=$nodeCount flow_edges=$relationshipCount distinct_pairs=$distinctPairs"
}

/** Raised when a projection cannot be built or resolved. */
final class ProjectionException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

/**
 * Builds and manages in-memory GDS graphs derived from the persisted demand
 * graph.
 *
 * Projection and drop are issued inside write transactions. GDS declares them
 * as read-mode procedures because they do not touch the store, but in a
 * cluster a read transaction may route to a follower and materialise the graph
 * on the wrong member; pinning to the leader keeps the catalogue coherent.
 */
final class GraphProjectionBuilder(connector: Neo4jConnector) extends StrictLogging {

  import GraphProjectionBuilder._

  // -------------------------------------------------------------------------
  // Catalogue inspection
  // -------------------------------------------------------------------------

  def exists(graphName: String): Boolean =
    connector
      .readOne("CALL gds.graph.exists($graphName) YIELD exists RETURN exists AS present", Map("graphName" -> graphName))(
        record => record.get("present").asBoolean(false)
      )
      .getOrElse(false)

  /** Drops a projection. Returns true when a graph was actually removed. */
  def drop(graphName: String): Boolean = {
    val dropped = runReturning(
      "CALL gds.graph.drop($graphName, false) YIELD graphName AS name RETURN name",
      Map("graphName" -> graphName)
    )(record => record.get("name").asString(""))

    val removed = dropped.nonEmpty
    if (removed) {
      logger.info(s"Dropped projection $graphName")
    }
    removed
  }

  /** Catalogue entry for one projection, if it is currently materialised. */
  def describe(graphName: String): Option[ProjectionSummary] =
    connector
      .readOne(
        """
          |CALL gds.graph.list($graphName)
          |YIELD graphName, nodeCount, relationshipCount, density, sizeInBytes, memoryUsage, degreeDistribution
          |RETURN graphName, nodeCount, relationshipCount, density, sizeInBytes, memoryUsage, degreeDistribution
          |""".stripMargin,
        Map("graphName" -> graphName)
      )(toSummary)

  /** Every projection currently held in the catalogue. */
  def list(): Vector[ProjectionSummary] =
    connector.read(
      """
        |CALL gds.graph.list()
        |YIELD graphName, nodeCount, relationshipCount, density, sizeInBytes, memoryUsage, degreeDistribution
        |RETURN graphName, nodeCount, relationshipCount, density, sizeInBytes, memoryUsage, degreeDistribution
        |ORDER BY graphName
        |""".stripMargin
    )(toSummary)

  /** Materialised generations of a logical projection, newest first. */
  def generationsOf(logicalName: String): Vector[(Long, String)] = {
    val prefix = generationPrefix(logicalName)
    list()
      .map(_.graphName)
      .filter(_.startsWith(prefix))
      .flatMap { name =>
        parseGeneration(name, prefix).map(generation => generation -> name)
      }
      .sortBy { case (generation, _) => -generation }
  }

  /** Physical name of the newest materialised generation, if any. */
  def activeGraphName(logicalName: String): Option[String] =
    generationsOf(logicalName).headOption.map { case (_, name) => name }

  // -------------------------------------------------------------------------
  // Pre-flight checks
  // -------------------------------------------------------------------------

  /**
   * Counts the store rows a projection would read. Worth calling before a wide
   * unbounded window, since GDS holds the whole slice in heap.
   */
  def sourceCardinality(spec: ProjectionSpec): SourceCardinality = {
    val label = CypherIdentifier.quote(spec.validatedNodeLabel)
    val relType = CypherIdentifier.quote(spec.validatedRelationshipType)

    val cypher =
      s"""
         |MATCH (source:$label)
         |${nodeFilterClause(spec, "source", "WHERE")}
         |WITH count(source) AS nodeCount
         |MATCH (origin:$label)-[flow:$relType]->(destination:$label)
         |WHERE ${flowPredicates(spec, "flow", "origin", "destination").mkString(" AND ")}
         |WITH nodeCount, count(flow) AS relationshipCount,
         |     count(DISTINCT [id(origin), id(destination)]) AS distinctPairs
         |RETURN nodeCount, relationshipCount, distinctPairs
         |""".stripMargin

    connector
      .readOne(cypher, projectionParameters(spec))(record =>
        SourceCardinality(
          nodeCount = record.get("nodeCount").asLong(0L),
          relationshipCount = record.get("relationshipCount").asLong(0L),
          distinctPairs = record.get("distinctPairs").asLong(0L)
        )
      )
      .getOrElse(SourceCardinality(0L, 0L, 0L))
  }

  /**
   * Native-projection memory estimate. The estimate is computed against the
   * full label and type rather than the window slice, so it is an upper bound
   * for a bounded projection.
   */
  def estimate(spec: ProjectionSpec): ProjectionEstimate = {
    val relationshipProjection = Map(
      spec.validatedRelationshipType -> Map(
        "type" -> spec.validatedRelationshipType,
        "orientation" -> spec.orientation.value,
        "aggregation" -> "SUM",
        "properties" -> spec.relationshipProperties.map { property =>
          property.validatedName -> Map(
            "property" -> property.validatedSource,
            "defaultValue" -> property.defaultValue,
            "aggregation" -> nativeAggregationFor(property)
          )
        }.toMap
      )
    )

    val nodeProjection = Map(
      spec.validatedNodeLabel -> Map(
        "label" -> spec.validatedNodeLabel,
        "properties" -> spec.nodeProperties.map { property =>
          property.validatedName -> Map(
            "property" -> property.validatedSource,
            "defaultValue" -> property.defaultValue
          )
        }.toMap
      )
    )

    connector
      .readOne(
        """
          |CALL gds.graph.project.estimate($nodeProjection, $relationshipProjection, {readConcurrency: $readConcurrency})
          |YIELD requiredMemory, bytesMin, bytesMax, nodeCount, relationshipCount
          |RETURN requiredMemory, bytesMin, bytesMax, nodeCount, relationshipCount
          |""".stripMargin,
        Map(
          "nodeProjection" -> nodeProjection,
          "relationshipProjection" -> relationshipProjection,
          "readConcurrency" -> spec.readConcurrency
        )
      )(record =>
        ProjectionEstimate(
          requiredMemory = record.get("requiredMemory").asString("unknown"),
          bytesMin = record.get("bytesMin").asLong(0L),
          bytesMax = record.get("bytesMax").asLong(0L),
          nodeCount = record.get("nodeCount").asLong(0L),
          relationshipCount = record.get("relationshipCount").asLong(0L)
        )
      )
      .getOrElse(throw new ProjectionException(s"Memory estimate returned no result for ${spec.describe}"))
  }

  // -------------------------------------------------------------------------
  // Projection
  // -------------------------------------------------------------------------

  /**
   * Materialises the next generation of `spec` and retires the generations
   * beyond the retention count. Returns the summary of the new graph.
   */
  def refresh(spec: ProjectionSpec): ProjectionSummary = {
    val existing = generationsOf(spec.validatedLogicalName)
    val nextGeneration = existing.headOption.map { case (generation, _) => generation + 1L }.getOrElse(1L)
    val graphName = physicalName(spec.validatedLogicalName, nextGeneration)

    if (exists(graphName)) {
      val _ = drop(graphName)
    }

    val summary = project(spec, graphName, nextGeneration)
    val retired = retireGenerations(spec.validatedLogicalName, nextGeneration, spec.retainGenerations)
    if (retired > 0) {
      logger.info(s"Retired $retired stale generation(s) of ${spec.validatedLogicalName}")
    }
    summary
  }

  /**
   * Materialises the projection under an explicit graph name, replacing any
   * graph already registered under it.
   */
  def project(spec: ProjectionSpec, graphName: String, generation: Long): ProjectionSummary = {
    val _ = drop(graphName)

    val cypher = buildProjectionQuery(spec)
    val parameters = projectionParameters(spec) ++ Map("graphName" -> graphName)

    logger.info(s"Projecting ${spec.describe} into $graphName")

    val projected = runReturning(cypher, parameters) { record =>
      val result = record.get("result")
      (
        result.get("nodeCount").asLong(0L),
        result.get("relationshipCount").asLong(0L),
        result.get("projectMillis").asLong(0L)
      )
    }.headOption

    projected match {
      case None =>
        throw new ProjectionException(s"Projection $graphName produced no catalogue entry for ${spec.describe}")

      case Some((nodeCount, relationshipCount, projectMillis)) =>
        val catalogued = describe(graphName).getOrElse(
          throw new ProjectionException(s"Projection $graphName is missing from the catalogue immediately after creation")
        )

        val summary = catalogued.copy(
          logicalName = spec.validatedLogicalName,
          generation = generation,
          projectMillis = projectMillis
        )

        if (summary.nodeCount != nodeCount || summary.relationshipCount != relationshipCount) {
          logger.warn(
            s"Catalogue counts differ from projection result for $graphName: " +
              s"catalogue=(${summary.nodeCount}, ${summary.relationshipCount}) " +
              s"projection=($nodeCount, $relationshipCount)"
          )
        }

        if (summary.relationshipCount == 0L) {
          logger.warn(s"Projection $graphName contains no relationships; check the window bounds and filters")
        }

        logger.info(s"Projection complete: ${summary.summary}")
        summary
    }
  }

  /**
   * Materialises a throwaway projection, runs `body` against its graph name,
   * and drops it afterwards regardless of outcome. Suitable for one-shot
   * analytics that should not accumulate in the catalogue.
   */
  def withProjection[T](spec: ProjectionSpec)(body: ProjectionSummary => T): T = {
    val graphName = physicalName(spec.validatedLogicalName, EphemeralGeneration)
    val summary = project(spec, graphName, EphemeralGeneration)
    try body(summary)
    finally {
      try {
        val _ = drop(graphName)
      } catch {
        case NonFatal(error) => logger.warn(s"Failed to drop ephemeral projection $graphName: ${error.getMessage}")
      }
    }
  }

  /** Drops every generation of a logical projection. Returns the count removed. */
  def dropAllGenerations(logicalName: String): Int = {
    val names = generationsOf(logicalName).map { case (_, name) => name }
    names.count(drop)
  }

  private def retireGenerations(logicalName: String, currentGeneration: Long, retain: Int): Int = {
    val stale = generationsOf(logicalName)
      .filter { case (generation, _) => generation != currentGeneration }
      .drop(math.max(0, retain - 1))
      .map { case (_, name) => name }

    stale.count(drop)
  }

  // -------------------------------------------------------------------------
  // Query construction
  // -------------------------------------------------------------------------

  /**
   * Builds the Cypher aggregation projection.
   *
   * Flow edges are collapsed per zone pair before the projection function is
   * called, because the streaming layer writes one edge per sliding window and
   * the analytics passes need a single weighted edge per pair. Isolated zones
   * are retained through the optional match so that centrality scores cover
   * every zone rather than only the connected ones.
   */
  private[graph] def buildProjectionQuery(spec: ProjectionSpec): String = {
    val label = CypherIdentifier.quote(spec.validatedNodeLabel)
    val relType = CypherIdentifier.quote(spec.validatedRelationshipType)

    val matchKeyword = if (spec.includeIsolatedNodes) "OPTIONAL MATCH" else "MATCH"
    val flowWhere = flowPredicates(spec, "flow", "source", "target").mkString("\n    AND ")

    val aggregations = spec.relationshipProperties.map(_.aggregationExpression("flow"))
    val aggregationClause =
      if (aggregations.isEmpty) "WITH source, target"
      else s"WITH source, target,\n     ${aggregations.mkString(",\n     ")}"

    val sourceNodeProperties = spec.nodeProperties.map(_.expression("source"))
    val targetNodeProperties = spec.nodeProperties.map(_.expression("target"))
    val relationshipEntries = spec.relationshipProperties.map(_.projectionEntry)

    val dataConfigEntries = Seq(
      Some(s"sourceNodeLabels: ['${spec.validatedNodeLabel}']"),
      Some(s"targetNodeLabels: ['${spec.validatedNodeLabel}']"),
      Some(s"relationshipType: '${spec.validatedRelationshipType}'"),
      if (sourceNodeProperties.isEmpty) None
      else Some(s"sourceNodeProperties: { ${sourceNodeProperties.mkString(", ")} }"),
      if (targetNodeProperties.isEmpty) None
      else Some(s"targetNodeProperties: { ${targetNodeProperties.mkString(", ")} }"),
      if (relationshipEntries.isEmpty) None
      else Some(s"relationshipProperties: { ${relationshipEntries.mkString(", ")} }")
    ).flatten

    // The Cypher aggregation projection has no orientation setting; a reverse
    // projection is expressed by swapping the endpoint arguments, and an
    // undirected one by naming the type in undirectedRelationshipTypes.
    val (projectionSource, projectionTarget) = spec.orientation match {
      case Orientation.Reverse => ("target", "source")
      case _ => ("source", "target")
    }

    val configuration = spec.orientation match {
      case Orientation.Undirected => s",\n    {{ undirectedRelationshipTypes: ['${spec.validatedRelationshipType}'] }}"
      case _ => ""
    }

    s"""
       |MATCH (source:$label)
       |${nodeFilterClause(spec, "source", "WHERE")}
       |$matchKeyword (source)-[flow:$relType]->(target:$label)
       |  WHERE $flowWhere
       |$aggregationClause
       |RETURN gds.graph.project(
       |  ${'$'}graphName,
       |  $projectionSource,
       |  $projectionTarget,
       |  {
       |    ${dataConfigEntries.mkString(",\n    ")}
       |  }$configuration
       |) AS result
       |""".stripMargin.replace("{{", "{").replace("}}", "}")
  }

  /** Predicates restricting which flow edges enter the projection. */
  private def flowPredicates(
      spec: ProjectionSpec,
      flowAlias: String,
      sourceAlias: String,
      targetAlias: String
  ): Seq[String] = {
    val windowLower = spec.windowFrom.map(_ => s"$flowAlias.windowStart >= ${'$'}windowFrom")
    val windowUpper = spec.windowTo.map(_ => s"$flowAlias.windowStart < ${'$'}windowTo")
    val tripThreshold =
      if (spec.minTripCount > 0L) Some(s"coalesce($flowAlias.tripCount, 0) >= ${'$'}minTripCount") else None
    val selfLoop = if (spec.includeInternalFlows) None else Some(s"$sourceAlias <> $targetAlias")
    val boroughs =
      if (spec.boroughFilter.isEmpty) None else Some(s"$targetAlias.borough IN ${'$'}boroughFilter")
    val serviceZones =
      if (spec.serviceZoneFilter.isEmpty) None else Some(s"$targetAlias.serviceZone IN ${'$'}serviceZoneFilter")

    val predicates = Seq(windowLower, windowUpper, tripThreshold, selfLoop, boroughs, serviceZones).flatten
    if (predicates.isEmpty) Seq("true") else predicates
  }

  /** Optional filter clause applied to the anchor node pattern. */
  private def nodeFilterClause(spec: ProjectionSpec, alias: String, keyword: String): String = {
    val boroughs = if (spec.boroughFilter.isEmpty) None else Some(s"$alias.borough IN ${'$'}boroughFilter")
    val serviceZones =
      if (spec.serviceZoneFilter.isEmpty) None else Some(s"$alias.serviceZone IN ${'$'}serviceZoneFilter")

    val predicates = Seq(boroughs, serviceZones).flatten
    if (predicates.isEmpty) "" else s"$keyword ${predicates.mkString(" AND ")}"
  }

  private def projectionParameters(spec: ProjectionSpec): Map[String, Any] = {
    val base = Map[String, Any](
      "minTripCount" -> spec.minTripCount,
      "readConcurrency" -> spec.readConcurrency
    )

    val withWindow = base ++
      spec.windowFrom.map(value => "windowFrom" -> value).toMap ++
      spec.windowTo.map(value => "windowTo" -> value).toMap

    val withBorough =
      if (spec.boroughFilter.isEmpty) withWindow else withWindow + ("boroughFilter" -> spec.boroughFilter)

    if (spec.serviceZoneFilter.isEmpty) withBorough
    else withBorough + ("serviceZoneFilter" -> spec.serviceZoneFilter)
  }

  private def nativeAggregationFor(property: RelationshipProperty): String =
    property.aggregation match {
      case PropertyAggregation.Sum => "SUM"
      case PropertyAggregation.Min => "MIN"
      case PropertyAggregation.Max => "MAX"
      case PropertyAggregation.Count => "COUNT"
      case PropertyAggregation.Avg => "SUM"
    }

  // -------------------------------------------------------------------------
  // Driver plumbing
  // -------------------------------------------------------------------------

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

  private def toSummary(record: Record): ProjectionSummary = {
    val graphName = record.get("graphName").asString("")
    ProjectionSummary(
      graphName = graphName,
      logicalName = logicalNameOf(graphName),
      generation = generationOf(graphName),
      nodeCount = record.get("nodeCount").asLong(0L),
      relationshipCount = record.get("relationshipCount").asLong(0L),
      density = record.get("density").asDouble(0.0),
      sizeInBytes = record.get("sizeInBytes").asLong(0L),
      memoryUsage = record.get("memoryUsage").asString("unknown"),
      degreeDistribution = toDoubleMap(record.get("degreeDistribution")),
      projectMillis = 0L
    )
  }

  private def toDoubleMap(value: Value): Map[String, Double] =
    if (value == null || value.isNull) {
      Map.empty
    } else {
      value
        .asMap(new JFunction[Value, java.lang.Double] {
          override def apply(entry: Value): java.lang.Double =
            if (entry == null || entry.isNull) java.lang.Double.valueOf(0.0)
            else java.lang.Double.valueOf(entry.asDouble(0.0))
        })
        .asScala
        .toMap
        .map { case (key, boxed) => key -> boxed.doubleValue() }
    }
}

object GraphProjectionBuilder {

  private val GenerationSeparator = "__g"
  private[graph] val EphemeralGeneration = 0L

  def apply(connector: Neo4jConnector): GraphProjectionBuilder = new GraphProjectionBuilder(connector)

  private[graph] def generationPrefix(logicalName: String): String = s"$logicalName$GenerationSeparator"

  private[graph] def physicalName(logicalName: String, generation: Long): String =
    s"${generationPrefix(logicalName)}$generation"

  private[graph] def parseGeneration(graphName: String, prefix: String): Option[Long] = {
    val suffix = graphName.substring(prefix.length)
    try Some(suffix.toLong)
    catch { case _: NumberFormatException => None }
  }

  private[graph] def logicalNameOf(graphName: String): String = {
    val separatorIndex = graphName.lastIndexOf(GenerationSeparator)
    if (separatorIndex <= 0) graphName else graphName.substring(0, separatorIndex)
  }

  private[graph] def generationOf(graphName: String): Long = {
    val separatorIndex = graphName.lastIndexOf(GenerationSeparator)
    if (separatorIndex <= 0) {
      EphemeralGeneration
    } else {
      parseGeneration(graphName, graphName.substring(0, separatorIndex + GenerationSeparator.length))
        .getOrElse(EphemeralGeneration)
    }
  }

  /**
   * Projection specifications used by the analytics passes over the demand
   * graph. Each is a starting point that callers narrow with a window.
   */
  object Specs {

    /** Trip volume between zones, weighted by aggregated trip count. */
    def zoneFlowVolume(windowFrom: Option[Long] = None, windowTo: Option[Long] = None): ProjectionSpec =
      ProjectionSpec(
        logicalName = "zone_flow_volume",
        windowFrom = windowFrom,
        windowTo = windowTo,
        minTripCount = 1L,
        orientation = Orientation.Natural,
        nodeProperties = Seq(
          NodeProperty("latestTripCount", "latestTripCount", 0.0),
          NodeProperty("peakTripCount", "peakTripCount", 0.0),
          NodeProperty("latestDemandPerMinute", "latestDemandPerMinute", 0.0)
        ),
        relationshipProperties = Seq(
          RelationshipProperty("tripCount", "tripCount", PropertyAggregation.Sum, 0.0),
          RelationshipProperty("totalRevenue", "totalRevenue", PropertyAggregation.Sum, 0.0),
          RelationshipProperty("avgDurationSeconds", "avgDurationSeconds", PropertyAggregation.Avg, 0.0)
        )
      )

    /**
     * Undirected view for community detection, where the direction of travel
     * is irrelevant to whether two zones belong to the same catchment.
     */
    def zoneCommunity(windowFrom: Option[Long] = None, windowTo: Option[Long] = None): ProjectionSpec =
      ProjectionSpec(
        logicalName = "zone_community",
        windowFrom = windowFrom,
        windowTo = windowTo,
        minTripCount = 5L,
        orientation = Orientation.Undirected,
        relationshipProperties = Seq(
          RelationshipProperty("tripCount", "tripCount", PropertyAggregation.Sum, 0.0)
        )
      )

    /**
     * Reversed view for inbound-pressure analysis: PageRank over this graph
     * ranks zones by how strongly the network pushes demand toward them.
     */
    def inboundPressure(windowFrom: Option[Long] = None, windowTo: Option[Long] = None): ProjectionSpec =
      ProjectionSpec(
        logicalName = "zone_inbound_pressure",
        windowFrom = windowFrom,
        windowTo = windowTo,
        minTripCount = 1L,
        orientation = Orientation.Reverse,
        relationshipProperties = Seq(
          RelationshipProperty("tripCount", "tripCount", PropertyAggregation.Sum, 0.0),
          RelationshipProperty("flowShare", "tripCount", PropertyAggregation.Count, 0.0)
        )
      )

    /** Travel-time weighted graph for shortest-path and reachability passes. */
    def travelTime(windowFrom: Option[Long] = None, windowTo: Option[Long] = None): ProjectionSpec =
      ProjectionSpec(
        logicalName = "zone_travel_time",
        windowFrom = windowFrom,
        windowTo = windowTo,
        minTripCount = 3L,
        orientation = Orientation.Natural,
        relationshipProperties = Seq(
          RelationshipProperty("avgDurationSeconds", "avgDurationSeconds", PropertyAggregation.Avg, 0.0),
          RelationshipProperty("avgTripDistanceMiles", "avgTripDistanceMiles", PropertyAggregation.Avg, 0.0),
          RelationshipProperty("tripCount", "tripCount", PropertyAggregation.Sum, 0.0)
        )
      )
  }
}
