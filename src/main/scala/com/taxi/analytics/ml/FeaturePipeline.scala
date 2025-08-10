package com.taxi.analytics.ml

import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import com.typesafe.scalalogging.StrictLogging

// ---------------------------------------------------------------------------
// Real-Time Graph Analytics & NYC Taxi Demand Forecasting
// Feature engineering and feature store
//
// Assembles a training matrix from three sources that live at different
// cadences:
//
//   Streaming metrics — one row per (zone, sliding window) written by the
//                       Structured Streaming job. Dense, high cardinality.
//   Graph topology    — one row per zone, refreshed whenever the GDS
//                       projection is rebuilt. Sparse, slow moving.
//   Flow structure    — one row per (origin, destination, window), used to
//                       derive inbound pressure per zone and window.
//
// Two correctness concerns dominate the design:
//
//   Grid completeness — lag features are defined in *slots*, not in rows. A
//                       zone with no trips in a window produces no row at all,
//                       so lagging over the raw table silently compares
//                       non-adjacent windows. The pipeline reifies the full
//                       (zone x slot) grid and fills absent demand with zero
//                       before any window function runs.
//
//   Leakage           — every rolling aggregate excludes the current row, and
//                       the target is a forward lead. Graph features are the
//                       harder case and are handled explicitly by
//                       GraphFeatureSource below.
// ---------------------------------------------------------------------------

/**
 * Where point-in-time graph features come from.
 *
 * Centrality scores and community identifiers are stored as current values on
 * the zone nodes. Joining those onto historical rows leaks information: the
 * PageRank that a zone has now was computed from trips that had not happened
 * at the time of the row being labelled. That is harmless for inference and
 * corrosive for backtesting, so the choice is explicit rather than implicit.
 */
sealed trait GraphFeatureSource extends Serializable

object GraphFeatureSource {

  /**
   * Reads the current property values from the `:Location` nodes. Correct for
   * inference, and acceptable for training only when the caller accepts that
   * backtest scores will be optimistic.
   */
  case object CurrentNodeProperties extends GraphFeatureSource

  /**
   * Reads dated snapshots of the graph features from a table written by the
   * projection refresh job, and joins each demand row to the most recent
   * snapshot that precedes it. This is the leakage-free path.
   *
   * @param path        snapshot table location
   * @param snapshotKey column holding the epoch-millis validity start of each snapshot
   */
  final case class Snapshots(path: String, snapshotKey: String = "snapshotStart") extends GraphFeatureSource

  /** Omits graph features entirely, for an ablation baseline. */
  case object Disabled extends GraphFeatureSource
}

/** Configuration for a feature build. */
final case class FeatureConfig(
    neo4jUri: String,
    neo4jUser: String,
    neo4jPassword: String,
    neo4jDatabase: String,
    slideSeconds: Int,
    windowSeconds: Int,
    horizonSlots: Int,
    lagSlots: Seq[Int],
    rollingSlots: Seq[Int],
    includeDailySeasonalLag: Boolean,
    includeWeeklySeasonalLag: Boolean,
    includeFlowFeatures: Boolean,
    graphFeatureSource: GraphFeatureSource,
    displayTimeZone: String,
    minObservedSlots: Int,
    readPartitions: Int,
    scaleFeatures: Boolean
) extends Serializable {

  require(slideSeconds >= 1, "slideSeconds must be at least 1")
  require(windowSeconds >= slideSeconds, "windowSeconds must be at least slideSeconds")
  require(horizonSlots >= 1, "horizonSlots must be at least 1")
  require(lagSlots.nonEmpty, "at least one lag must be configured")
  require(lagSlots.forall(_ >= 1), "lags must be positive")
  require(lagSlots.distinct.size == lagSlots.size, "lags must be unique")
  require(rollingSlots.forall(_ >= 2), "rolling spans must cover at least two slots")
  require(rollingSlots.distinct.size == rollingSlots.size, "rolling spans must be unique")
  require(minObservedSlots >= 0, "minObservedSlots must be non-negative")
  require(readPartitions >= 1, "readPartitions must be at least 1")

  val slideMillis: Long = slideSeconds.toLong * 1000L

  /** Slots covering one day; the daily seasonal lag needs this to be whole. */
  val slotsPerDay: Int = (86400 / slideSeconds).toInt

  /** Slots covering one week. */
  val slotsPerWeek: Int = slotsPerDay * 7

  require(
    !includeDailySeasonalLag || 86400 % slideSeconds == 0,
    "a daily seasonal lag requires slideSeconds to divide 86400 exactly"
  )

  /** Furthest backward reach of any feature, used to trim the warm-up region. */
  val maxLookbackSlots: Int = {
    val lagReach = lagSlots.max
    val rollingReach = if (rollingSlots.isEmpty) 0 else rollingSlots.max
    val dailyReach = if (includeDailySeasonalLag) slotsPerDay else 0
    val weeklyReach = if (includeWeeklySeasonalLag) slotsPerWeek else 0
    Seq(lagReach, rollingReach, dailyReach, weeklyReach).max
  }

  def describe: String =
    s"slide=${slideSeconds}s window=${windowSeconds}s horizon=$horizonSlots lags=${lagSlots.mkString(",")} " +
      s"rolling=${rollingSlots.mkString(",")} lookback=$maxLookbackSlots graph=$graphFeatureSource"
}

object FeatureConfig {

  /**
   * Matches the streaming job's default 15-minute window on a 5-minute slide,
   * forecasting three slots ahead.
   */
  def default(uri: String, user: String, password: String, database: String = "neo4j"): FeatureConfig =
    FeatureConfig(
      neo4jUri = uri,
      neo4jUser = user,
      neo4jPassword = password,
      neo4jDatabase = database,
      slideSeconds = 300,
      windowSeconds = 900,
      horizonSlots = 3,
      lagSlots = Seq(1, 2, 3, 6, 12),
      rollingSlots = Seq(6, 12, 36),
      includeDailySeasonalLag = true,
      includeWeeklySeasonalLag = true,
      includeFlowFeatures = true,
      graphFeatureSource = GraphFeatureSource.CurrentNodeProperties,
      displayTimeZone = "America/New_York",
      minObservedSlots = 1,
      readPartitions = 8,
      scaleFeatures = false
    )
}

/** Canonical column names for the feature matrix. */
object FeatureColumns {

  val LocationId = "location_id"
  val WindowStart = "window_start"
  val WindowEnd = "window_end"

  val Target = "target_trip_count"
  val TargetWindowStart = "target_window_start"
  val FeatureVector = "features"
  val ScaledFeatureVector = "scaled_features"
  val PartitionDay = "window_day"

  // Contemporaneous streaming metrics.
  val TripCount = "trip_count"
  val PassengerTotal = "passenger_total"
  val DemandPerMinute = "demand_per_minute"
  val AvgTripDistance = "avg_trip_distance_miles"
  val AvgDuration = "avg_duration_seconds"
  val AvgSpeed = "avg_speed_mph"
  val AvgFare = "avg_fare_amount"
  val TotalRevenue = "total_revenue"
  val TotalTips = "total_tips"
  val DistinctDestinations = "distinct_destinations"
  val SurgeRatio = "surge_ratio"
  val BaselineDemand = "baseline_demand_per_minute"
  val IsSurge = "is_surge"

  // Calendar encodings.
  val HourOfDay = "hour_of_day"
  val MinuteOfDay = "minute_of_day"
  val DayOfWeek = "day_of_week"
  val IsWeekend = "is_weekend"
  val HourSin = "hour_sin"
  val HourCos = "hour_cos"
  val WeekdaySin = "weekday_sin"
  val WeekdayCos = "weekday_cos"

  // Graph topology.
  val PageRank = "page_rank_score"
  val Betweenness = "betweenness_score"
  val Importance = "importance_score"
  val ImportanceRank = "importance_rank"
  val CorridorBias = "corridor_bias"
  val CommunityId = "community_id"
  val CommunitySize = "community_size"
  val CommunityCohesion = "community_cohesion"
  val CommunityPeerDemand = "community_peer_mean_demand"
  val CommunityShare = "community_demand_share"

  // Flow structure.
  val InboundTrips = "inbound_trip_count"
  val InboundZones = "inbound_zone_count"
  val OutboundTrips = "outbound_trip_count"
  val OutboundZones = "outbound_zone_count"
  val NetFlow = "net_flow"

  val Keys: Seq[String] = Seq(LocationId, WindowStart)

  /** Columns carried through the pipeline but excluded from the model input. */
  val NonFeatureColumns: Seq[String] =
    Seq(LocationId, WindowStart, WindowEnd, Target, TargetWindowStart, PartitionDay, FeatureVector, ScaledFeatureVector)
}

/**
 * Builds model-ready feature rows from the demand graph.
 *
 * Bulk reads go through the Neo4j Spark connector rather than the Bolt data
 * access layer: the DAL is tuned for small, latency-sensitive result sets,
 * while a training build sweeps millions of window rows and needs partitioned
 * parallel reads.
 */
final class FeaturePipeline(spark: SparkSession, config: FeatureConfig) extends StrictLogging {

  import FeatureColumns._
  import spark.implicits._

  // -------------------------------------------------------------------------
  // Source loading
  // -------------------------------------------------------------------------

  /** Reads demand windows, optionally bounded to an event-time range. */
  def loadDemandWindows(windowFrom: Option[Long], windowTo: Option[Long]): DataFrame = {
    val raw = neo4jReader
      .option("labels", "DemandWindow")
      .option("partitions", config.readPartitions.toString)
      .load()

    val bounded = (windowFrom, windowTo) match {
      case (Some(from), Some(to)) => raw.filter(col("windowStart") >= from && col("windowStart") < to)
      case (Some(from), None) => raw.filter(col("windowStart") >= from)
      case (None, Some(to)) => raw.filter(col("windowStart") < to)
      case (None, None) => raw
    }

    bounded
      .select(
        col("locationId").cast(IntegerType).as(LocationId),
        col("windowStart").cast(LongType).as(WindowStart),
        col("windowEnd").cast(LongType).as(WindowEnd),
        col("tripCount").cast(DoubleType).as(TripCount),
        col("passengerTotal").cast(DoubleType).as(PassengerTotal),
        col("demandPerMinute").cast(DoubleType).as(DemandPerMinute),
        col("avgTripDistanceMiles").cast(DoubleType).as(AvgTripDistance),
        col("avgDurationSeconds").cast(DoubleType).as(AvgDuration),
        col("avgSpeedMph").cast(DoubleType).as(AvgSpeed),
        col("avgFareAmount").cast(DoubleType).as(AvgFare),
        col("totalRevenue").cast(DoubleType).as(TotalRevenue),
        col("totalTips").cast(DoubleType).as(TotalTips),
        col("distinctDestinations").cast(DoubleType).as(DistinctDestinations),
        col("surgeRatio").cast(DoubleType).as(SurgeRatio),
        col("baselineDemandPerMinute").cast(DoubleType).as(BaselineDemand),
        when(col("isSurge"), lit(1.0)).otherwise(lit(0.0)).as(IsSurge)
      )
      .dropDuplicates(LocationId, WindowStart)
  }

  /** Reads the zone dimension together with its current graph topology properties. */
  def loadZoneGraphFeatures(): DataFrame =
    neo4jReader
      .option("labels", "Location")
      .option("partitions", "1")
      .load()
      .select(
        col("locationId").cast(IntegerType).as(LocationId),
        coalesce(col("pageRankScore"), lit(0.0)).cast(DoubleType).as(PageRank),
        coalesce(col("betweennessScore"), lit(0.0)).cast(DoubleType).as(Betweenness),
        coalesce(col("importanceScore"), lit(0.0)).cast(DoubleType).as(Importance),
        coalesce(col("importanceRank"), lit(0.0)).cast(DoubleType).as(ImportanceRank),
        coalesce(col("corridorBias"), lit(0.0)).cast(DoubleType).as(CorridorBias),
        coalesce(col("communityId"), lit(-1L)).cast(LongType).as(CommunityId)
      )
      .dropDuplicates(LocationId)

  /** Reads community-level aggregates written by the clustering pass. */
  def loadCommunityFeatures(): DataFrame =
    neo4jReader
      .option("labels", "ZoneCommunity")
      .option("partitions", "1")
      .load()
      .select(
        col("communityId").cast(LongType).as(CommunityId),
        coalesce(col("size"), lit(0.0)).cast(DoubleType).as(CommunitySize),
        coalesce(col("cohesion"), lit(0.0)).cast(DoubleType).as(CommunityCohesion)
      )
      .dropDuplicates(CommunityId)

  /**
   * Reads flow edges and folds them into per-zone inbound and outbound
   * pressure for each window.
   */
  def loadFlowFeatures(windowFrom: Option[Long], windowTo: Option[Long]): DataFrame = {
    val raw = neo4jReader
      .option("relationship", "TRIP_FLOW")
      .option("relationship.source.labels", ":Location")
      .option("relationship.target.labels", ":Location")
      .option("relationship.nodes.map", "false")
      .option("partitions", config.readPartitions.toString)
      .load()

    val bounded = (windowFrom, windowTo) match {
      case (Some(from), Some(to)) => raw.filter(col("`rel.windowStart`") >= from && col("`rel.windowStart`") < to)
      case (Some(from), None) => raw.filter(col("`rel.windowStart`") >= from)
      case (None, Some(to)) => raw.filter(col("`rel.windowStart`") < to)
      case (None, None) => raw
    }

    val edges = bounded.select(
      col("`source.locationId`").cast(IntegerType).as("origin_id"),
      col("`target.locationId`").cast(IntegerType).as("destination_id"),
      col("`rel.windowStart`").cast(LongType).as(WindowStart),
      coalesce(col("`rel.tripCount`"), lit(0.0)).cast(DoubleType).as("edge_trips")
    )

    val inbound = edges
      .groupBy(col("destination_id").as(LocationId), col(WindowStart))
      .agg(
        sum("edge_trips").as(InboundTrips),
        countDistinct("origin_id").cast(DoubleType).as(InboundZones)
      )

    val outbound = edges
      .groupBy(col("origin_id").as(LocationId), col(WindowStart))
      .agg(
        sum("edge_trips").as(OutboundTrips),
        countDistinct("destination_id").cast(DoubleType).as(OutboundZones)
      )

    inbound
      .join(outbound, Seq(LocationId, WindowStart), "full_outer")
      .na
      .fill(0.0, Seq(InboundTrips, InboundZones, OutboundTrips, OutboundZones))
      .withColumn(NetFlow, col(InboundTrips) - col(OutboundTrips))
  }

  private def neo4jReader =
    spark.read
      .format("org.neo4j.spark.DataSource")
      .option("url", config.neo4jUri)
      .option("authentication.type", "basic")
      .option("authentication.basic.username", config.neo4jUser)
      .option("authentication.basic.password", config.neo4jPassword)
      .option("database", config.neo4jDatabase)

  // -------------------------------------------------------------------------
  // Grid completion
  // -------------------------------------------------------------------------

  /**
   * Expands the observed rows onto the complete (zone x slot) grid.
   *
   * Absent rows mean zero demand, not missing data, and lag features are only
   * meaningful once every slot exists. Zones with fewer than
   * `minObservedSlots` real observations are dropped rather than fabricated:
   * a zone that never appears in the source is out of service, and filling it
   * with zeros would teach the model a constant.
   */
  def completeGrid(demand: DataFrame, zones: DataFrame): DataFrame = {
    val bounds = demand.agg(min(col(WindowStart)).as("lo"), max(col(WindowStart)).as("hi")).collect()

    if (bounds.isEmpty || bounds.head.isNullAt(0)) {
      logger.warn("Demand source is empty; returning an empty grid")
      demand.limit(0)
    } else {
      val lowerBound = bounds.head.getLong(0)
      val upperBound = bounds.head.getLong(1)
      val slotCount = ((upperBound - lowerBound) / config.slideMillis) + 1L

      logger.info(s"Grid bounds span $slotCount slots at ${config.slideSeconds}s")

      val activeZones = demand
        .groupBy(col(LocationId))
        .agg(count(lit(1)).as("observed_slots"))
        .filter(col("observed_slots") >= config.minObservedSlots)
        .select(col(LocationId))
        .join(zones.select(col(LocationId)), Seq(LocationId), "inner")

      val slots = spark
        .range(0L, slotCount)
        .select((lit(lowerBound) + col("id") * lit(config.slideMillis)).as(WindowStart))

      val grid = activeZones.crossJoin(slots)

      val filled = grid
        .join(demand, Seq(LocationId, WindowStart), "left_outer")
        .withColumn(WindowEnd, coalesce(col(WindowEnd), col(WindowStart) + lit(config.windowSeconds.toLong * 1000L)))

      val zeroFilled = Seq(
        TripCount, PassengerTotal, DemandPerMinute, TotalRevenue, TotalTips,
        DistinctDestinations, IsSurge
      )

      // Rate and average columns are genuinely undefined in an empty window;
      // carrying the zone's last known value forward is more faithful than
      // asserting zero mileage or zero speed.
      val carriedForward = Seq(AvgTripDistance, AvgDuration, AvgSpeed, AvgFare, BaselineDemand, SurgeRatio)

      val carryWindow = Window
        .partitionBy(col(LocationId))
        .orderBy(col(WindowStart))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

      val withCarry = carriedForward.foldLeft(filled) { (frame, column) =>
        frame.withColumn(column, coalesce(last(col(column), ignoreNulls = true).over(carryWindow), lit(0.0)))
      }

      withCarry.na.fill(0.0, zeroFilled)
    }
  }

  // -------------------------------------------------------------------------
  // Feature groups
  // -------------------------------------------------------------------------

  /**
   * Cyclical calendar encodings derived from the window boundary.
   *
   * Hour and weekday are encoded as sine/cosine pairs so that the model sees
   * the wraparound: the last slot of a day sits adjacent to the first, which a
   * raw integer hour would place maximally far apart.
   */
  def withCalendarFeatures(frame: DataFrame): DataFrame = {
    val localTime = from_utc_timestamp(timestamp_millis(col(WindowStart)), config.displayTimeZone)

    val hour = hour(localTime).cast(DoubleType)
    val minuteOfDay = (hour * lit(60.0)) + minute(localTime).cast(DoubleType)
    val weekday = (dayofweek(localTime) - lit(1)).cast(DoubleType)

    frame
      .withColumn(HourOfDay, hour)
      .withColumn(MinuteOfDay, minuteOfDay)
      .withColumn(DayOfWeek, weekday)
      .withColumn(IsWeekend, when(col(DayOfWeek).isin(0.0, 6.0), lit(1.0)).otherwise(lit(0.0)))
      .withColumn(HourSin, sin(col(MinuteOfDay) * lit(2.0 * math.Pi / 1440.0)))
      .withColumn(HourCos, cos(col(MinuteOfDay) * lit(2.0 * math.Pi / 1440.0)))
      .withColumn(WeekdaySin, sin(col(DayOfWeek) * lit(2.0 * math.Pi / 7.0)))
      .withColumn(WeekdayCos, cos(col(DayOfWeek) * lit(2.0 * math.Pi / 7.0)))
  }

  /**
   * Backward lags of the demand signals. Every lag reaches strictly into the
   * past, so no lag column can see the row it describes.
   */
  def withLagFeatures(frame: DataFrame): DataFrame = {
    val ordering = Window.partitionBy(col(LocationId)).orderBy(col(WindowStart))

    val shortLags = config.lagSlots.foldLeft(frame) { (current, slots) =>
      current
        .withColumn(s"${TripCount}_lag_$slots", coalesce(lag(col(TripCount), slots).over(ordering), lit(0.0)))
        .withColumn(s"${DemandPerMinute}_lag_$slots", coalesce(lag(col(DemandPerMinute), slots).over(ordering), lit(0.0)))
    }

    val withDaily =
      if (!config.includeDailySeasonalLag) shortLags
      else
        shortLags.withColumn(
          s"${TripCount}_lag_day",
          coalesce(lag(col(TripCount), config.slotsPerDay).over(ordering), lit(0.0))
        )

    if (!config.includeWeeklySeasonalLag) withDaily
    else
      withDaily.withColumn(
        s"${TripCount}_lag_week",
        coalesce(lag(col(TripCount), config.slotsPerWeek).over(ordering), lit(0.0))
      )
  }

  /**
   * Trailing aggregates over each rolling span.
   *
   * The frame is `rowsBetween(-span, -1)`, deliberately excluding the current
   * row: including it would let the mean carry the value the model is trying
   * to explain, which inflates validation scores and collapses in production.
   */
  def withRollingFeatures(frame: DataFrame): DataFrame =
    config.rollingSlots.foldLeft(frame) { (current, span) =>
      val trailing = Window
        .partitionBy(col(LocationId))
        .orderBy(col(WindowStart))
        .rowsBetween(-span.toLong, -1L)

      current
        .withColumn(s"${TripCount}_mean_$span", coalesce(avg(col(TripCount)).over(trailing), lit(0.0)))
        .withColumn(s"${TripCount}_stddev_$span", coalesce(stddev_samp(col(TripCount)).over(trailing), lit(0.0)))
        .withColumn(s"${TripCount}_max_$span", coalesce(max(col(TripCount)).over(trailing), lit(0.0)))
        .withColumn(s"${TripCount}_min_$span", coalesce(min(col(TripCount)).over(trailing), lit(0.0)))
        .withColumn(
          s"${TripCount}_trend_$span",
          col(TripCount) - coalesce(avg(col(TripCount)).over(trailing), lit(0.0))
        )
    }

  /** Joins graph topology onto the demand rows according to the configured source. */
  def withGraphFeatures(frame: DataFrame): DataFrame =
    config.graphFeatureSource match {
      case GraphFeatureSource.Disabled =>
        logger.info("Graph features disabled; building a demand-only matrix")
        frame

      case GraphFeatureSource.CurrentNodeProperties =>
        logger.warn(
          "Joining current graph properties onto historical rows. Valid for inference; " +
            "backtest scores built this way are optimistic because centrality reflects future trips."
        )
        val zoneFeatures = loadZoneGraphFeatures()
        val communityFeatures = loadCommunityFeatures()

        frame
          .join(broadcast(zoneFeatures), Seq(LocationId), "left_outer")
          .join(broadcast(communityFeatures), Seq(CommunityId), "left_outer")
          .na
          .fill(0.0, Seq(PageRank, Betweenness, Importance, ImportanceRank, CorridorBias, CommunitySize, CommunityCohesion))
          .na
          .fill(-1L, Seq(CommunityId))

      case GraphFeatureSource.Snapshots(path, snapshotKey) =>
        val snapshots = spark.read.parquet(path)
        asOfJoinSnapshots(frame, snapshots, snapshotKey)
    }

  /**
   * As-of join against graph feature snapshots: each demand row takes the most
   * recent snapshot whose validity start does not exceed the row's window
   * start. Implemented as a ranked join rather than a range join so that a
   * zone missing from an early snapshot simply has no match instead of picking
   * up a later one.
   */
  private def asOfJoinSnapshots(frame: DataFrame, snapshots: DataFrame, snapshotKey: String): DataFrame = {
    val candidates = frame
      .select(col(LocationId), col(WindowStart))
      .join(snapshots, Seq(LocationId), "left_outer")
      .filter(col(snapshotKey).isNull || col(snapshotKey) <= col(WindowStart))

    val ranked = Window
      .partitionBy(col(LocationId), col(WindowStart))
      .orderBy(col(snapshotKey).desc_nulls_last)

    val nearest = candidates
      .withColumn("snapshot_rank", row_number().over(ranked))
      .filter(col("snapshot_rank") === 1)
      .drop("snapshot_rank", snapshotKey)

    frame
      .join(nearest, Seq(LocationId, WindowStart), "left_outer")
      .na
      .fill(0.0, Seq(PageRank, Betweenness, Importance, ImportanceRank, CorridorBias, CommunitySize, CommunityCohesion))
      .na
      .fill(-1L, Seq(CommunityId))
  }

  /**
   * Contemporaneous demand of the zone's community peers.
   *
   * The peer mean excludes the zone itself, so it carries neighbourhood
   * pressure rather than an echo of the zone's own signal. Both quantities are
   * measured at feature time, which precedes the target, so no leakage is
   * introduced.
   */
  def withCommunityPeerFeatures(frame: DataFrame): DataFrame =
    if (!frame.columns.contains(CommunityId)) {
      frame
    } else {
      val peers = Window.partitionBy(col(CommunityId), col(WindowStart))

      val communityTotal = sum(col(TripCount)).over(peers)
      val communityMembers = count(lit(1)).over(peers)

      frame
        .withColumn(
          CommunityPeerDemand,
          when(communityMembers > 1, (communityTotal - col(TripCount)) / (communityMembers - lit(1)))
            .otherwise(lit(0.0))
        )
        .withColumn(
          CommunityShare,
          when(communityTotal > 0.0, col(TripCount) / communityTotal).otherwise(lit(0.0))
        )
    }

  /** Joins per-window inbound and outbound pressure onto the grid. */
  def withFlowFeatures(frame: DataFrame, flows: DataFrame): DataFrame =
    frame
      .join(flows, Seq(LocationId, WindowStart), "left_outer")
      .na
      .fill(0.0, Seq(InboundTrips, InboundZones, OutboundTrips, OutboundZones, NetFlow))

  /**
   * Attaches the forecast target: trip count `horizonSlots` ahead. Rows whose
   * target falls past the end of the observed range are dropped, since a null
   * label is not a zero label.
   */
  def withTarget(frame: DataFrame): DataFrame = {
    val ordering = Window.partitionBy(col(LocationId)).orderBy(col(WindowStart))

    frame
      .withColumn(Target, lead(col(TripCount), config.horizonSlots).over(ordering))
      .withColumn(TargetWindowStart, lead(col(WindowStart), config.horizonSlots).over(ordering))
      .filter(col(Target).isNotNull)
  }

  /**
   * Trims the warm-up region where lag and rolling columns are still padded
   * with zeros. Training on those rows teaches the model that a zero history
   * is a real state.
   */
  def trimWarmUp(frame: DataFrame): DataFrame = {
    val bounds = frame.agg(min(col(WindowStart)).as("lo")).collect()
    if (bounds.isEmpty || bounds.head.isNullAt(0)) {
      frame
    } else {
      val firstSlot = bounds.head.getLong(0)
      val cutoff = firstSlot + (config.maxLookbackSlots.toLong * config.slideMillis)
      logger.info(s"Trimming ${config.maxLookbackSlots} warm-up slots per zone")
      frame.filter(col(WindowStart) >= cutoff)
    }
  }

  // -------------------------------------------------------------------------
  // Assembly
  // -------------------------------------------------------------------------

  /** Feature column names present on a built frame, in a stable order. */
  def featureNames(frame: DataFrame): Seq[String] =
    frame.columns.toSeq
      .filterNot(FeatureColumns.NonFeatureColumns.contains)
      .sorted

  /**
   * Assembles the numeric columns into a single vector column. `handleInvalid`
   * is set to `error` on purpose: a null reaching the assembler means an
   * earlier join or fill was wrong, and silently skipping the row would hide
   * that.
   */
  def assembleVector(frame: DataFrame): DataFrame = {
    val inputs = featureNames(frame).toArray

    val assembler = new VectorAssembler()
      .setInputCols(inputs)
      .setOutputCol(FeatureVector)
      .setHandleInvalid("error")

    val assembled = assembler.transform(frame)

    if (!config.scaleFeatures) {
      assembled
    } else {
      val scaler = new StandardScaler()
        .setInputCol(FeatureVector)
        .setOutputCol(ScaledFeatureVector)
        .setWithMean(true)
        .setWithStd(true)

      scaler.fit(assembled).transform(assembled)
    }
  }

  // -------------------------------------------------------------------------
  // Entry points
  // -------------------------------------------------------------------------

  /**
   * Builds a labelled training matrix over an event-time range.
   *
   * Flow features are loaded across a widened range so that the edges backing
   * the first retained slots are present rather than truncated by the caller's
   * bound.
   */
  def buildTrainingSet(windowFrom: Option[Long], windowTo: Option[Long]): DataFrame = {
    logger.info(s"Building training matrix: ${config.describe}")

    val demand = loadDemandWindows(windowFrom, windowTo)
    val zones = loadZoneGraphFeatures().select(col(LocationId))

    val grid = completeGrid(demand, zones)
    val enriched = pipelineStages(grid, windowFrom, windowTo)

    val labelled = trimWarmUp(withTarget(enriched))

    labelled
      .withColumn(PartitionDay, (col(WindowStart) / lit(86400000L)).cast(LongType))
      .repartition(col(PartitionDay))
  }

  /**
   * Builds an unlabelled matrix for the most recent slots, ready for scoring.
   *
   * The lookback is extended behind the requested horizon so that the lag and
   * rolling columns of the retained rows are backed by real history rather
   * than padding.
   */
  def buildInferenceSet(latestWindowStart: Long, slotsToScore: Int = 1): DataFrame = {
    require(slotsToScore >= 1, "slotsToScore must be at least 1")

    val lookbackSlots = config.maxLookbackSlots + slotsToScore
    val windowFrom = latestWindowStart - (lookbackSlots.toLong * config.slideMillis)
    val windowTo = latestWindowStart + config.slideMillis

    logger.info(s"Building inference matrix over $lookbackSlots lookback slots")

    val demand = loadDemandWindows(Some(windowFrom), Some(windowTo))
    val zones = loadZoneGraphFeatures().select(col(LocationId))

    val grid = completeGrid(demand, zones)
    val enriched = pipelineStages(grid, Some(windowFrom), Some(windowTo))

    val scoringCutoff = latestWindowStart - ((slotsToScore - 1).toLong * config.slideMillis)

    enriched
      .filter(col(WindowStart) >= scoringCutoff)
      .withColumn(PartitionDay, (col(WindowStart) / lit(86400000L)).cast(LongType))
  }

  /** Shared enrichment chain used by both entry points. */
  private def pipelineStages(grid: DataFrame, windowFrom: Option[Long], windowTo: Option[Long]): DataFrame = {
    val calendar = withCalendarFeatures(grid)
    val lagged = withLagFeatures(calendar)
    val rolled = withRollingFeatures(lagged)
    val graphed = withGraphFeatures(rolled)
    val peered = withCommunityPeerFeatures(graphed)

    if (!config.includeFlowFeatures) {
      peered
    } else {
      val flows = loadFlowFeatures(windowFrom, windowTo)
      withFlowFeatures(peered, flows)
    }
  }

  // -------------------------------------------------------------------------
  // Feature store
  // -------------------------------------------------------------------------

  /**
   * Writes the matrix to the feature store, partitioned by day slot so that a
   * retrain over a narrower range prunes rather than scans.
   */
  def writeFeatureStore(frame: DataFrame, path: String, mode: SaveMode = SaveMode.Overwrite): Unit = {
    val partitioned =
      if (frame.columns.contains(PartitionDay)) frame
      else frame.withColumn(PartitionDay, (col(WindowStart) / lit(86400000L)).cast(LongType))

    partitioned.write
      .mode(mode)
      .partitionBy(PartitionDay)
      .option("compression", "snappy")
      .parquet(path)

    logger.info(s"Feature store written to $path")
  }

  /** Reads the feature store back, optionally pruned to a day-slot range. */
  def readFeatureStore(path: String, fromDaySlot: Option[Long] = None, toDaySlot: Option[Long] = None): DataFrame = {
    val raw = spark.read.parquet(path)
    (fromDaySlot, toDaySlot) match {
      case (Some(from), Some(to)) => raw.filter(col(PartitionDay) >= from && col(PartitionDay) < to)
      case (Some(from), None) => raw.filter(col(PartitionDay) >= from)
      case (None, Some(to)) => raw.filter(col(PartitionDay) < to)
      case (None, None) => raw
    }
  }

  /**
   * Writes a snapshot of the current graph topology, stamped with the event
   * time from which it is valid. Calling this on every projection refresh is
   * what makes GraphFeatureSource.Snapshots usable later.
   */
  def writeGraphSnapshot(path: String, validFrom: Long, snapshotKey: String = "snapshotStart"): Unit = {
    val snapshot = loadZoneGraphFeatures()
      .join(broadcast(loadCommunityFeatures()), Seq(CommunityId), "left_outer")
      .na
      .fill(0.0, Seq(CommunitySize, CommunityCohesion))
      .withColumn(snapshotKey, lit(validFrom).cast(LongType))

    snapshot.write
      .mode(SaveMode.Append)
      .option("compression", "snappy")
      .parquet(path)

    logger.info(s"Graph feature snapshot appended to $path")
  }

  /**
   * Per-column null and variance report. A constant column carries no signal
   * and usually indicates an upstream join that silently produced defaults.
   */
  def profileFeatures(frame: DataFrame): Seq[(String, Long, Double)] = {
    val columns = featureNames(frame)
    if (columns.isEmpty) {
      Seq.empty
    } else {
      val aggregations: Seq[Column] = columns.flatMap { column =>
        Seq(
          sum(when(col(column).isNull, lit(1L)).otherwise(lit(0L))).as(s"${column}__nulls"),
          coalesce(stddev_samp(col(column)), lit(0.0)).as(s"${column}__stddev")
        )
      }

      val row = frame.agg(aggregations.head, aggregations.tail: _*).collect().head

      columns.map { column =>
        val nulls = row.getAs[Long](s"${column}__nulls")
        val deviation = row.getAs[Double](s"${column}__stddev")
        (column, nulls, deviation)
      }
    }
  }

  /** Logs the columns that are constant or partially null. */
  def logFeatureHealth(frame: DataFrame): Unit = {
    val profile = profileFeatures(frame)

    val constant = profile.collect { case (name, _, deviation) if deviation == 0.0 => name }
    val nullable = profile.collect { case (name, nulls, _) if nulls > 0L => s"$name=$nulls" }

    if (constant.nonEmpty) {
      logger.warn(s"Constant feature columns carry no signal: ${constant.mkString(", ")}")
    }
    if (nullable.nonEmpty) {
      logger.warn(s"Null values survived the fill stage: ${nullable.mkString(", ")}")
    }
    logger.info(s"Feature matrix: ${profile.size} feature columns")
  }

  /** Chronological split point, expressed as the fraction of slots kept for training. */
  def chronologicalSplit(frame: DataFrame, trainFraction: Double): (DataFrame, DataFrame) = {
    require(trainFraction > 0.0 && trainFraction < 1.0, "trainFraction must be within (0.0, 1.0)")

    val bounds = frame.agg(min(col(WindowStart)).as("lo"), max(col(WindowStart)).as("hi")).collect()
    if (bounds.isEmpty || bounds.head.isNullAt(0)) {
      (frame, frame.limit(0))
    } else {
      val lowerBound = bounds.head.getLong(0)
      val upperBound = bounds.head.getLong(1)
      val cutoff = lowerBound + ((upperBound - lowerBound).toDouble * trainFraction).toLong

      // The holdout starts a full horizon after the cutoff so that no training
      // row's target overlaps the evaluation period.
      val embargo = cutoff + (config.horizonSlots.toLong * config.slideMillis)

      logger.info(s"Chronological split at slot $cutoff with a ${config.horizonSlots}-slot embargo")

      (frame.filter(col(WindowStart) < cutoff), frame.filter(col(WindowStart) >= embargo))
    }
  }
}

object FeaturePipeline {

  def apply(spark: SparkSession, config: FeatureConfig): FeaturePipeline =
    new FeaturePipeline(spark, config)
}
