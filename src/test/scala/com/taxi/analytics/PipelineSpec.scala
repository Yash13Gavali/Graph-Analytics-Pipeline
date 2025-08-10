package com.taxi.analytics

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.{Instant, ZoneId, ZonedDateTime}

import scala.util.control.NonFatal

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Tag
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.taxi.analytics.database.{Neo4jConfig, Neo4jConnector, ParameterEncoder, RetryPolicy}
import com.taxi.analytics.graph.{
  CypherIdentifier,
  GraphProjectionBuilder,
  ImportanceWeights,
  LouvainConfig,
  NodeProperty,
  Orientation,
  PageRankConfig,
  ProjectionSpec,
  PropertyAggregation,
  RelationshipProperty,
  ZoneImportance
}
import com.taxi.analytics.ingestion.{ProducerSettings, TripEvent}
import com.taxi.analytics.ml.{
  DemandForecaster,
  FeatureColumns,
  FeatureConfig,
  FeaturePipeline,
  ForecastMetrics,
  FoldResult,
  GbtParams,
  GraphFeatureSource,
  ModelFamily,
  TrainingConfig,
  ValidationReport
}
import com.taxi.analytics.processor.ProcessorSettings

/** Marks tests that require a reachable Neo4j instance. */
object Neo4jIntegration extends Tag("com.taxi.analytics.Neo4jIntegration")

/**
 * Real-Time Graph Analytics & NYC Taxi Demand Forecasting — test suite.
 *
 * Three tiers, deliberately separated by what they need to run:
 *
 *   Pure       — configuration parsing, validation rules and arithmetic. No
 *                Spark, no database, milliseconds to run.
 *   Spark      — feature transforms and model metrics against a local
 *                session. The leakage guarantees live here, because a lag or
 *                rolling frame that quietly reaches forward produces a model
 *                that validates beautifully and fails in production.
 *   Integration — anything needing a live graph. Cancelled rather than failed
 *                when no instance is reachable, so `sbt test` stays green on a
 *                machine without Docker while still exercising the real driver
 *                when one is available.
 *
 * The Neo4j driver does not open a connection at construction, only at first
 * use, so a connector can be built offline to exercise the pure methods that
 * hang off it.
 */
class PipelineSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  import PipelineSpec._

  private var sparkSession: SparkSession = _
  private var tempRoot: Path = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession
      .builder()
      .appName("pipeline-spec")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("WARN")
    tempRoot = Files.createTempDirectory("pipeline-spec")
  }

  override def afterAll(): Unit = {
    try {
      if (sparkSession != null) {
        sparkSession.stop()
      }
      if (tempRoot != null) {
        deleteRecursively(tempRoot)
      }
    } finally {
      super.afterAll()
    }
  }

  private def spark: SparkSession = sparkSession

  // =========================================================================
  // Tier 1: pure configuration and arithmetic
  // =========================================================================

  describe("IntervalSeconds") {

    it("parses every supported unit") {
      IntervalSeconds.parse("30 seconds") shouldBe 30
      IntervalSeconds.parse("5 minutes") shouldBe 300
      IntervalSeconds.parse("2 hours") shouldBe 7200
    }

    it("accepts singular unit names") {
      IntervalSeconds.parse("1 minute") shouldBe 60
      IntervalSeconds.parse("1 hour") shouldBe 3600
    }

    it("rejects malformed specifications") {
      IntervalSeconds.isValid("5m") shouldBe false
      IntervalSeconds.isValid("minutes") shouldBe false
      IntervalSeconds.isValid("") shouldBe false

      an[IllegalArgumentException] should be thrownBy IntervalSeconds.parse("5m")
    }
  }

  describe("DriverMode") {

    it("resolves every declared mode by name") {
      DriverMode.Modes.foreach { mode =>
        DriverMode.fromString(mode.name) shouldBe Right(mode)
      }
    }

    it("is case insensitive and trims") {
      DriverMode.fromString("  ALL  ") shouldBe Right(DriverMode.All)
    }

    it("rejects unknown modes with a message naming the alternatives") {
      val outcome = DriverMode.fromString("backfill")
      outcome.isLeft shouldBe true
      outcome.left.toOption.get should include("stream")
    }
  }

  describe("DriverSettings.parse") {

    it("rejects positional arguments") {
      DriverSettings.parse(Array("graph")).isLeft shouldBe true
    }

    it("rejects a flag with no value") {
      val outcome = DriverSettings.parse(Array("--mode"))
      outcome.isLeft shouldBe true
      outcome.left.toOption.get should include("Missing value")
    }

    it("returns usage text for --help") {
      DriverSettings.parse(Array("--help")) shouldBe Left(DriverSettings.Usage)
    }

    it("requires a mode") {
      val outcome = DriverSettings.parse(Array("--topic", "nyc-taxi-trips"))
      outcome.isLeft shouldBe true
      outcome.left.toOption.get should include("--mode")
    }
  }

  describe("ProducerSettings.parse") {

    it("requires an input path") {
      val outcome = ProducerSettings.parse(Array("--topic", "nyc-taxi-trips"))
      outcome.isLeft shouldBe true
      outcome.left.toOption.get should include("--input")
    }

    it("rejects an input path that does not exist") {
      val missing = tempRoot.resolve("absent.csv").toString
      val outcome = ProducerSettings.parse(Array("--input", missing))
      outcome.isLeft shouldBe true
      outcome.left.toOption.get should include("does not exist")
    }

    it("accepts a readable file and applies documented defaults") {
      val source = writeTempFile("trips.csv", "header\n")
      val outcome = ProducerSettings.parse(Array("--input", source.toString))

      outcome.isRight shouldBe true
      val settings = outcome.toOption.get
      settings.topic shouldBe "nyc-taxi-trips"
      settings.serviceType shouldBe "yellow"
      settings.recordsPerSecond shouldBe 0
      settings.sourceTimeZone shouldBe ZoneId.of("America/New_York")
    }

    it("rejects an unknown service type") {
      val source = writeTempFile("service.csv", "header\n")
      val outcome = ProducerSettings.parse(
        Array("--input", source.toString, "--service-type", "helicopter")
      )
      outcome.isLeft shouldBe true
    }

    it("rejects a delivery timeout shorter than the request timeout plus linger") {
      val source = writeTempFile("timeouts.csv", "header\n")
      val outcome = ProducerSettings.parse(
        Array(
          "--input", source.toString,
          "--request-timeout-ms", "30000",
          "--linger-ms", "1000",
          "--delivery-timeout-ms", "5000"
        )
      )
      outcome.isLeft shouldBe true
      outcome.left.toOption.get should include("delivery-timeout-ms")
    }
  }

  describe("ProcessorSettings.parse") {

    it("accepts an explicit password and applies defaults") {
      val outcome = ProcessorSettings.parse(Array("--neo4j-password", "test-secret"))

      outcome.isRight shouldBe true
      val settings = outcome.toOption.get
      settings.topic shouldBe "nyc-taxi-trips"
      settings.windowDuration shouldBe "15 minutes"
      settings.slideDuration shouldBe "5 minutes"
      settings.neo4j.password shouldBe "test-secret"
    }

    it("rejects a slide longer than the window") {
      val outcome = ProcessorSettings.parse(
        Array(
          "--neo4j-password", "test-secret",
          "--window-duration", "5 minutes",
          "--slide-duration", "15 minutes"
        )
      )
      outcome.isLeft shouldBe true
      outcome.left.toOption.get should include("slide-duration")
    }

    it("rejects an unsupported output mode") {
      val outcome = ProcessorSettings.parse(
        Array("--neo4j-password", "test-secret", "--output-mode", "complete")
      )
      outcome.isLeft shouldBe true
    }
  }

  describe("RetryPolicy") {

    it("grows the backoff exponentially and caps it") {
      val policy = RetryPolicy(maxAttempts = 8, 100L, 1000L, 2.0, jitterFactor = 0.0)

      policy.backoffFor(1) shouldBe 100L
      policy.backoffFor(2) shouldBe 200L
      policy.backoffFor(3) shouldBe 400L
      policy.backoffFor(7) shouldBe 1000L
      policy.backoffFor(20) shouldBe 1000L
    }

    it("keeps jittered backoff inside the configured band") {
      val policy = RetryPolicy(maxAttempts = 5, 1000L, 4000L, 2.0, jitterFactor = 0.2)

      (1 to 200).foreach { _ =>
        val backoff = policy.backoffFor(2)
        backoff should be >= 1600L
        backoff should be <= 2400L
      }
    }

    it("rejects incoherent policies") {
      an[IllegalArgumentException] should be thrownBy RetryPolicy(0, 100L, 1000L, 2.0, 0.0)
      an[IllegalArgumentException] should be thrownBy RetryPolicy(3, 1000L, 100L, 2.0, 0.0)
      an[IllegalArgumentException] should be thrownBy RetryPolicy(3, 100L, 1000L, 0.5, 0.0)
      an[IllegalArgumentException] should be thrownBy RetryPolicy(3, 100L, 1000L, 2.0, 1.5)
    }
  }

  describe("Neo4jConfig") {

    it("keys pooled instances on uri, database and principal") {
      val base = testNeo4jConfig("bolt://host:7687", "neo4j")
      base.instanceKey shouldBe base.instanceKey
      base.instanceKey should not be testNeo4jConfig("bolt://host:7687", "other").instanceKey
      base.instanceKey should not be testNeo4jConfig("bolt://elsewhere:7687", "neo4j").instanceKey
    }

    it("detects schemes that negotiate TLS themselves") {
      testNeo4jConfig("neo4j+s://host", "neo4j").schemeManagesEncryption shouldBe true
      testNeo4jConfig("bolt+ssc://host", "neo4j").schemeManagesEncryption shouldBe true
      testNeo4jConfig("bolt://host:7687", "neo4j").schemeManagesEncryption shouldBe false
    }

    it("keeps the password out of the log rendering") {
      testNeo4jConfig("bolt://host:7687", "neo4j").describe should not include "secret"
    }
  }

  describe("ParameterEncoder") {

    it("widens every integral type to Long so Bolt sees one numeric shape") {
      ParameterEncoder.encode(7) shouldBe java.lang.Long.valueOf(7L)
      ParameterEncoder.encode(7L) shouldBe java.lang.Long.valueOf(7L)
      ParameterEncoder.encode(7.toShort) shouldBe java.lang.Long.valueOf(7L)
    }

    it("widens floating types to Double") {
      ParameterEncoder.encode(1.5f) shouldBe java.lang.Double.valueOf(1.5)
      ParameterEncoder.encode(1.5d) shouldBe java.lang.Double.valueOf(1.5)
    }

    it("maps absent options onto Cypher null") {
      ParameterEncoder.encode(None) shouldBe null
      ParameterEncoder.encode(null) shouldBe null
      ParameterEncoder.encode(Some(3)) shouldBe java.lang.Long.valueOf(3L)
    }

    it("encodes nested collections") {
      val encoded = ParameterEncoder.encodeMap(
        Map("ids" -> Seq(1, 2, 3), "nested" -> Map("depth" -> 2))
      )

      encoded.get("ids").asInstanceOf[java.util.List[_]].size() shouldBe 3
      encoded
        .get("nested")
        .asInstanceOf[java.util.Map[String, Object]]
        .get("depth") shouldBe java.lang.Long.valueOf(2L)
    }

    it("encodes a batch of rows into a list of maps") {
      val rows = ParameterEncoder.encodeRows(Seq(Map("a" -> 1), Map("a" -> 2)))
      rows.size() shouldBe 2
      rows.get(1).get("a") shouldBe java.lang.Long.valueOf(2L)
    }
  }

  describe("CypherIdentifier") {

    it("accepts identifiers that Cypher can carry unquoted") {
      CypherIdentifier.validate("label", "Location").isRight shouldBe true
      CypherIdentifier.validate("property", "trip_count").isRight shouldBe true
      CypherIdentifier.validate("property", "_internal").isRight shouldBe true
    }

    it("rejects anything that could break out of an interpolated statement") {
      Seq("Location; MATCH (n) DETACH DELETE n", "trip count", "1count", "`quoted`", "").foreach {
        candidate =>
          CypherIdentifier.validate("label", candidate).isLeft shouldBe true
      }
    }

    it("throws on an invalid identifier when required") {
      an[IllegalArgumentException] should be thrownBy CypherIdentifier.require("label", "bad label")
    }

    it("backtick-quotes for embedding") {
      CypherIdentifier.quote("trip_count") shouldBe "`trip_count`"
    }
  }

  describe("Orientation and PropertyAggregation") {

    it("resolves orientations by name, case insensitively") {
      Orientation.fromString("undirected") shouldBe Right(Orientation.Undirected)
      Orientation.fromString(" REVERSE ") shouldBe Right(Orientation.Reverse)
      Orientation.fromString("sideways").isLeft shouldBe true
    }

    it("resolves aggregations by name") {
      PropertyAggregation.fromString("sum") shouldBe Right(PropertyAggregation.Sum)
      PropertyAggregation.fromString("median").isLeft shouldBe true
    }
  }

  describe("ProjectionSpec") {

    it("rejects labels and types that are not valid identifiers") {
      an[IllegalArgumentException] should be thrownBy ProjectionSpec(
        logicalName = "flows",
        nodeLabel = "Location) DETACH DELETE (n"
      )
    }

    it("rejects an inverted window") {
      an[IllegalArgumentException] should be thrownBy ProjectionSpec(
        logicalName = "flows",
        windowFrom = Some(2000L),
        windowTo = Some(1000L)
      )
    }

    it("rejects duplicate relationship property names") {
      an[IllegalArgumentException] should be thrownBy ProjectionSpec(
        logicalName = "flows",
        relationshipProperties = Seq(
          RelationshipProperty("weight", "tripCount", PropertyAggregation.Sum, 0.0),
          RelationshipProperty("weight", "totalRevenue", PropertyAggregation.Sum, 0.0)
        )
      )
    }

    it("rejects duplicate node property names") {
      an[IllegalArgumentException] should be thrownBy ProjectionSpec(
        logicalName = "flows",
        nodeProperties = Seq(
          NodeProperty("weight", "latestTripCount", 0.0),
          NodeProperty("weight", "peakTripCount", 0.0)
        )
      )
    }

    it("renders an unbounded window distinctly from a bounded one") {
      ProjectionSpec("flows").describe should include("unbounded")
      ProjectionSpec("flows", windowFrom = Some(1L), windowTo = Some(2L)).describe should include("[1,2)")
    }
  }

  describe("GraphProjectionBuilder.Specs") {

    it("projects the community graph undirected, as Louvain requires") {
      GraphProjectionBuilder.Specs.zoneCommunity().orientation shouldBe Orientation.Undirected
    }

    it("reverses the inbound pressure graph so PageRank ranks destinations") {
      GraphProjectionBuilder.Specs.inboundPressure().orientation shouldBe Orientation.Reverse
    }

    it("carries a trip count weight on the volume projection") {
      val spec = GraphProjectionBuilder.Specs.zoneFlowVolume()
      spec.relationshipProperties.map(_.validatedName) should contain("tripCount")
      spec.orientation shouldBe Orientation.Natural
    }

    it("propagates the requested window onto the spec") {
      val spec = GraphProjectionBuilder.Specs.zoneFlowVolume(Some(100L), Some(200L))
      spec.windowFrom shouldBe Some(100L)
      spec.windowTo shouldBe Some(200L)
    }
  }

  describe("LouvainConfig") {

    it("rejects consecutive ids combined with a seed property") {
      an[IllegalArgumentException] should be thrownBy LouvainConfig(
        consecutiveIds = true,
        seedProperty = Some("communityId")
      )
    }

    it("reports whether a run carries identity forward") {
      LouvainConfig().isSeeded shouldBe false
      LouvainConfig(seedProperty = Some("communityId")).isSeeded shouldBe true
    }

    it("rejects an invalid seed property name") {
      an[IllegalArgumentException] should be thrownBy LouvainConfig(seedProperty = Some("bad name"))
    }
  }

  describe("PageRankConfig") {

    it("rejects a damping factor outside the open unit interval") {
      an[IllegalArgumentException] should be thrownBy PageRankConfig(dampingFactor = 1.0)
      an[IllegalArgumentException] should be thrownBy PageRankConfig(dampingFactor = 0.0)
    }

    it("marks a seeded run as personalised") {
      PageRankConfig().isPersonalised shouldBe false
      PageRankConfig(sourceLocationIds = Seq(132, 138)).isPersonalised shouldBe true
    }
  }

  describe("ImportanceWeights") {

    it("normalises to a unit sum regardless of the raw scale") {
      val weights = ImportanceWeights(pageRank = 6.5, betweenness = 3.5)
      (weights.normalisedPageRank + weights.normalisedBetweenness) shouldBe 1.0 +- Tolerance
      weights.normalisedPageRank shouldBe 0.65 +- Tolerance
    }

    it("rejects a degenerate weighting") {
      an[IllegalArgumentException] should be thrownBy ImportanceWeights(0.0, 0.0)
      an[IllegalArgumentException] should be thrownBy ImportanceWeights(-1.0, 2.0)
    }
  }

  describe("ZoneImportance") {

    it("reports a positive corridor bias when betweenness leads page rank") {
      val corridor = zoneImportance(normalisedPageRank = 0.2, normalisedBetweenness = 0.8)
      corridor.corridorBias shouldBe 0.6 +- Tolerance

      val terminal = zoneImportance(normalisedPageRank = 0.9, normalisedBetweenness = 0.1)
      terminal.corridorBias should be < 0.0
    }
  }

  describe("TripEvent") {

    it("keys on the pickup zone so per-zone ordering survives the topic") {
      testTripEvent(pickupZone = 132, dropoffZone = 138).partitionKey shouldBe "132"
    }

    it("emits snake_case field names") {
      val json = testTripEvent(132, 138).asJson.noSpaces

      Seq(
        "\"event_id\"",
        "\"pickup_location_id\"",
        "\"dropoff_location_id\"",
        "\"trip_duration_seconds\"",
        "\"pickup_epoch_millis\""
      ).foreach(field => json should include(field))
    }

    it("renders absent optional values as JSON null rather than omitting them") {
      val json = testTripEvent(132, 138).asJson
      json.hcursor.downField("vendor_id").focus.map(_.isNull) shouldBe Some(true)
    }
  }

  describe("ForecastMetrics") {

    it("reports bias as the signed gap between mean prediction and mean label") {
      val overForecasting = ForecastMetrics(100L, 2.0, 1.5, 0.8, 12.0, 0.4, 10.0, 12.0)
      overForecasting.bias shouldBe 2.0 +- Tolerance

      val underForecasting = overForecasting.copy(meanPrediction = 8.0)
      underForecasting.bias shouldBe -2.0 +- Tolerance
    }

    it("normalises rmse against the mean level and guards a zero level") {
      ForecastMetrics(10L, 4.0, 3.0, 0.5, 20.0, 1.0, 8.0, 8.0).normalisedRmse shouldBe 0.5 +- Tolerance
      ForecastMetrics(10L, 4.0, 3.0, 0.5, 20.0, 1.0, 0.0, 0.0).normalisedRmse shouldBe 0.0
    }
  }

  describe("FoldResult and ValidationReport") {

    it("scores skill as the fraction of baseline error removed") {
      val fold = testFold(1, modelRmse = 5.0, baselineRmse = 10.0)
      fold.skillScore shouldBe 0.5 +- Tolerance

      testFold(2, modelRmse = 12.0, baselineRmse = 10.0).skillScore should be < 0.0
      testFold(3, modelRmse = 5.0, baselineRmse = 0.0).skillScore shouldBe 0.0
    }

    it("aggregates fold metrics and flags a model that fails to beat the baseline") {
      val beating = ValidationReport(Seq(testFold(1, 4.0, 8.0), testFold(2, 6.0, 8.0)))
      beating.meanRmse shouldBe 5.0 +- Tolerance
      beating.beatsBaseline shouldBe true

      val losing = ValidationReport(Seq(testFold(1, 9.0, 8.0), testFold(2, 10.0, 8.0)))
      losing.beatsBaseline shouldBe false
    }

    it("reports zero spread for a single fold and a real spread beyond that") {
      ValidationReport(Seq(testFold(1, 5.0, 8.0))).rmseStdDev shouldBe 0.0
      ValidationReport(Seq(testFold(1, 4.0, 8.0), testFold(2, 6.0, 8.0))).rmseStdDev shouldBe 1.4142 +- 0.001
    }

    it("returns neutral aggregates for an empty report") {
      val empty = ValidationReport(Seq.empty)
      empty.meanRmse shouldBe 0.0
      empty.meanSkill shouldBe 0.0
      empty.beatsBaseline shouldBe false
    }
  }

  describe("FeatureConfig") {

    it("derives slot counts from the slide interval") {
      val config = testFeatureConfig()
      config.slotsPerDay shouldBe 288
      config.slotsPerWeek shouldBe 2016
      config.slideMillis shouldBe 300000L
    }

    it("takes the maximum reach of every backward-looking feature as the lookback") {
      testFeatureConfig().maxLookbackSlots shouldBe 12

      val seasonal = testFeatureConfig().copy(includeDailySeasonalLag = true)
      seasonal.maxLookbackSlots shouldBe 288
    }

    it("rejects a daily seasonal lag when the slide does not divide the day") {
      an[IllegalArgumentException] should be thrownBy testFeatureConfig().copy(
        slideSeconds = 700,
        includeDailySeasonalLag = true
      )
    }

    it("rejects a window shorter than the slide") {
      an[IllegalArgumentException] should be thrownBy testFeatureConfig().copy(
        slideSeconds = 900,
        windowSeconds = 300
      )
    }
  }

  describe("TrainingConfig") {

    it("rejects residual quantile probabilities outside the open unit interval") {
      an[IllegalArgumentException] should be thrownBy TrainingConfig(
        residualQuantileProbabilities = Seq(0.0, 0.5)
      )
      an[IllegalArgumentException] should be thrownBy TrainingConfig(
        residualQuantileProbabilities = Seq(0.5, 0.5)
      )
    }

    it("rejects a negative embargo") {
      an[IllegalArgumentException] should be thrownBy TrainingConfig(embargoMillis = -1L)
    }
  }

  describe("GbtParams") {

    it("rejects a step size outside the unit interval and an unknown loss") {
      an[IllegalArgumentException] should be thrownBy GbtParams(stepSize = 0.0)
      an[IllegalArgumentException] should be thrownBy GbtParams(stepSize = 1.5)
      an[IllegalArgumentException] should be thrownBy GbtParams(lossType = "huber")
    }
  }

  // =========================================================================
  // Tier 2: feature transforms against a local Spark session
  // =========================================================================

  describe("FeaturePipeline.completeGrid") {

    it("reifies every zone and slot in the observed range") {
      val pipeline = new FeaturePipeline(spark, testFeatureConfig())
      val demand = sparseDemandFrame(spark)
      val zones = zoneFrame(spark, Seq(1, 2))

      val grid = pipeline.completeGrid(demand, zones)

      // Two zones across six contiguous slots, regardless of which were observed.
      grid.count() shouldBe 12L
      grid.select(FeatureColumns.WindowStart).distinct().count() shouldBe 6L
    }

    it("fills absent counts with zero because no row means no trips") {
      val pipeline = new FeaturePipeline(spark, testFeatureConfig())
      val grid = pipeline.completeGrid(sparseDemandFrame(spark), zoneFrame(spark, Seq(1, 2)))

      val gap = grid
        .filter(col(FeatureColumns.LocationId) === 1 && col(FeatureColumns.WindowStart) === BaseSlot + 2 * SlideMillis)
        .select(FeatureColumns.TripCount)
        .collect()

      gap.length shouldBe 1
      gap.head.getDouble(0) shouldBe 0.0
    }

    it("carries rate columns forward rather than asserting a zero speed") {
      val pipeline = new FeaturePipeline(spark, testFeatureConfig())
      val grid = pipeline.completeGrid(sparseDemandFrame(spark), zoneFrame(spark, Seq(1, 2)))

      val carried = grid
        .filter(col(FeatureColumns.LocationId) === 1 && col(FeatureColumns.WindowStart) === BaseSlot + 2 * SlideMillis)
        .select(FeatureColumns.AvgSpeed)
        .collect()
        .head
        .getDouble(0)

      // The last observed speed for zone 1 before the gap, not zero.
      carried shouldBe 12.0 +- Tolerance
    }

    it("drops zones with fewer observations than the configured floor") {
      val config = testFeatureConfig().copy(minObservedSlots = 3)
      val pipeline = new FeaturePipeline(spark, config)
      val grid = pipeline.completeGrid(sparseDemandFrame(spark), zoneFrame(spark, Seq(1, 2)))

      val retained = grid.select(FeatureColumns.LocationId).distinct().collect().map(_.getInt(0)).toSet
      retained should contain(1)
      retained should not contain 2
    }
  }

  describe("FeaturePipeline.withCalendarFeatures") {

    it("derives the local hour from the window boundary") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val frame = pipeline.withCalendarFeatures(syntheticMatrix(spark, config, zones = 1, slots = 4))

      val row = frame.orderBy(FeatureColumns.WindowStart).select(FeatureColumns.WindowStart, FeatureColumns.HourOfDay).collect().head
      val slot = row.getLong(0)
      val expected = ZonedDateTime
        .ofInstant(Instant.ofEpochMilli(slot), ZoneId.of(config.displayTimeZone))
        .getHour
        .toDouble

      row.getDouble(1) shouldBe expected
    }

    it("keeps cyclical encodings inside the unit circle") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val frame = pipeline.withCalendarFeatures(syntheticMatrix(spark, config, zones = 2, slots = 300))

      val bounds = frame
        .agg(
          min(FeatureColumns.HourSin).as("min_sin"),
          max(FeatureColumns.HourSin).as("max_sin"),
          min(FeatureColumns.WeekdayCos).as("min_cos"),
          max(FeatureColumns.WeekdayCos).as("max_cos")
        )
        .collect()
        .head

      bounds.getDouble(0) should be >= -1.0
      bounds.getDouble(1) should be <= 1.0
      bounds.getDouble(2) should be >= -1.0
      bounds.getDouble(3) should be <= 1.0
    }

    it("marks weekend slots consistently with the calendar") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val frame = pipeline.withCalendarFeatures(syntheticMatrix(spark, config, zones = 1, slots = 600))

      val mismatches = frame
        .filter(
          (col(FeatureColumns.DayOfWeek).isin(0.0, 6.0) && col(FeatureColumns.IsWeekend) =!= 1.0) ||
            (!col(FeatureColumns.DayOfWeek).isin(0.0, 6.0) && col(FeatureColumns.IsWeekend) =!= 0.0)
        )
        .count()

      mismatches shouldBe 0L
    }
  }

  describe("FeaturePipeline.withLagFeatures") {

    it("aligns each lag with the corresponding earlier slot") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val lagged = pipeline.withLagFeatures(syntheticMatrix(spark, config, zones = 1, slots = 20))

      val rows = lagged
        .orderBy(FeatureColumns.WindowStart)
        .select(FeatureColumns.TripCount, s"${FeatureColumns.TripCount}_lag_1", s"${FeatureColumns.TripCount}_lag_3")
        .collect()

      rows.indices.drop(3).foreach { index =>
        rows(index).getDouble(1) shouldBe rows(index - 1).getDouble(0)
        rows(index).getDouble(2) shouldBe rows(index - 3).getDouble(0)
      }
    }

    it("pads the leading rows with zero rather than null") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val lagged = pipeline.withLagFeatures(syntheticMatrix(spark, config, zones = 1, slots = 20))

      lagged.filter(col(s"${FeatureColumns.TripCount}_lag_1").isNull).count() shouldBe 0L
    }

    it("never crosses a zone boundary" ) {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val lagged = pipeline.withLagFeatures(syntheticMatrix(spark, config, zones = 3, slots = 20))

      val firstSlot = lagged.agg(min(FeatureColumns.WindowStart)).collect().head.getLong(0)
      val leadingRows = lagged
        .filter(col(FeatureColumns.WindowStart) === firstSlot)
        .select(s"${FeatureColumns.TripCount}_lag_1")
        .collect()

      leadingRows should have length 3
      leadingRows.foreach(_.getDouble(0) shouldBe 0.0)
    }
  }

  describe("FeaturePipeline.withRollingFeatures") {

    it("excludes the current row from every trailing aggregate") {
      val config = testFeatureConfig().copy(rollingSlots = Seq(6))
      val pipeline = new FeaturePipeline(spark, config)

      // A single enormous spike: if the rolling mean saw the current row, the
      // spike's own aggregate would jump with it. That is the leakage this
      // guards against, and it is the difference between a model that
      // validates well and one that works.
      val base = constantDemandFrame(spark, zones = 1, slots = 20, tripCount = 10.0)
      val spiked = base.withColumn(
        FeatureColumns.TripCount,
        when(col(FeatureColumns.WindowStart) === BaseSlot + 10 * SlideMillis, lit(1000.0))
          .otherwise(col(FeatureColumns.TripCount))
      )

      val rolled = pipeline.withRollingFeatures(spiked)

      val spikeRow = rolled
        .filter(col(FeatureColumns.WindowStart) === BaseSlot + 10 * SlideMillis)
        .select(s"${FeatureColumns.TripCount}_mean_6", s"${FeatureColumns.TripCount}_max_6")
        .collect()
        .head

      spikeRow.getDouble(0) shouldBe 10.0 +- Tolerance
      spikeRow.getDouble(1) shouldBe 10.0 +- Tolerance
    }

    it("reflects the spike only in the following rows") {
      val config = testFeatureConfig().copy(rollingSlots = Seq(6))
      val pipeline = new FeaturePipeline(spark, config)

      val base = constantDemandFrame(spark, zones = 1, slots = 20, tripCount = 10.0)
      val spiked = base.withColumn(
        FeatureColumns.TripCount,
        when(col(FeatureColumns.WindowStart) === BaseSlot + 10 * SlideMillis, lit(1000.0))
          .otherwise(col(FeatureColumns.TripCount))
      )

      val nextMax = pipeline
        .withRollingFeatures(spiked)
        .filter(col(FeatureColumns.WindowStart) === BaseSlot + 11 * SlideMillis)
        .select(s"${FeatureColumns.TripCount}_max_6")
        .collect()
        .head
        .getDouble(0)

      nextMax shouldBe 1000.0 +- Tolerance
    }
  }

  describe("FeaturePipeline.withTarget") {

    it("labels each row with the count one horizon ahead") {
      val config = testFeatureConfig().copy(horizonSlots = 3)
      val pipeline = new FeaturePipeline(spark, config)
      val labelled = pipeline.withTarget(syntheticMatrix(spark, config, zones = 1, slots = 20))

      val rows = labelled
        .orderBy(FeatureColumns.WindowStart)
        .select(FeatureColumns.WindowStart, FeatureColumns.Target, FeatureColumns.TargetWindowStart)
        .collect()

      rows.foreach { row =>
        row.getLong(2) shouldBe row.getLong(0) + 3 * SlideMillis
      }
    }

    it("drops the trailing rows whose target falls outside the observed range") {
      val config = testFeatureConfig().copy(horizonSlots = 3)
      val pipeline = new FeaturePipeline(spark, config)
      val source = syntheticMatrix(spark, config, zones = 2, slots = 20)

      pipeline.withTarget(source).count() shouldBe (source.count() - 2 * 3)
    }

    it("never emits a null label") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val labelled = pipeline.withTarget(syntheticMatrix(spark, config, zones = 2, slots = 20))

      labelled.filter(col(FeatureColumns.Target).isNull).count() shouldBe 0L
    }
  }

  describe("FeaturePipeline.trimWarmUp") {

    it("removes the leading slots where lag columns are still padded") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val source = syntheticMatrix(spark, config, zones = 1, slots = 40)

      val trimmed = pipeline.trimWarmUp(source)
      val earliest = trimmed.agg(min(FeatureColumns.WindowStart)).collect().head.getLong(0)

      earliest shouldBe BaseSlot + config.maxLookbackSlots.toLong * SlideMillis
      trimmed.count() shouldBe (40L - config.maxLookbackSlots)
    }
  }

  describe("FeaturePipeline.chronologicalSplit") {

    it("keeps the holdout strictly after the training range") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val (training, holdout) = pipeline.chronologicalSplit(
        syntheticMatrix(spark, config, zones = 2, slots = 200),
        trainFraction = 0.7
      )

      val trainMax = training.agg(max(FeatureColumns.WindowStart)).collect().head.getLong(0)
      val holdoutMin = holdout.agg(min(FeatureColumns.WindowStart)).collect().head.getLong(0)

      holdoutMin should be > trainMax
    }

    it("embargoes a full horizon so no training target reaches into the holdout") {
      val config = testFeatureConfig().copy(horizonSlots = 6)
      val pipeline = new FeaturePipeline(spark, config)
      val (training, holdout) = pipeline.chronologicalSplit(
        syntheticMatrix(spark, config, zones = 2, slots = 200),
        trainFraction = 0.7
      )

      val trainMax = training.agg(max(FeatureColumns.WindowStart)).collect().head.getLong(0)
      val holdoutMin = holdout.agg(min(FeatureColumns.WindowStart)).collect().head.getLong(0)

      (holdoutMin - trainMax) should be >= config.horizonSlots.toLong * SlideMillis
    }

    it("rejects a fraction outside the open unit interval") {
      val pipeline = new FeaturePipeline(spark, testFeatureConfig())
      val frame = syntheticMatrix(spark, testFeatureConfig(), zones = 1, slots = 10)

      an[IllegalArgumentException] should be thrownBy pipeline.chronologicalSplit(frame, 0.0)
      an[IllegalArgumentException] should be thrownBy pipeline.chronologicalSplit(frame, 1.0)
    }
  }

  describe("FeaturePipeline.assembleVector") {

    it("assembles exactly the feature columns and no bookkeeping columns") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val labelled = pipeline.withTarget(pipeline.withLagFeatures(syntheticMatrix(spark, config, zones = 2, slots = 40)))

      val names = pipeline.featureNames(labelled)
      names should not contain FeatureColumns.LocationId
      names should not contain FeatureColumns.Target
      names should not contain FeatureColumns.WindowStart

      val assembled = pipeline.assembleVector(labelled)
      assembled.columns should contain(FeatureColumns.FeatureVector)

      val vector = assembled.select(FeatureColumns.FeatureVector).head().getAs[org.apache.spark.ml.linalg.Vector](0)
      vector.size shouldBe names.size
    }

    it("reports a constant column through the health profile") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val frame = syntheticMatrix(spark, config, zones = 2, slots = 40)
        .withColumn("constant_column", lit(1.0))

      val profile = pipeline.profileFeatures(frame).toMap.map { case (name, _) => name }
      profile should contain("constant_column")

      val deviations = pipeline.profileFeatures(frame).collect {
        case (name, _, stddev) if name == "constant_column" => stddev
      }
      deviations.head shouldBe 0.0
    }
  }

  // =========================================================================
  // Tier 2b: model evaluation
  // =========================================================================

  describe("DemandForecaster.baselineMetrics") {

    it("computes persistence error exactly when no daily lag is present") {
      val forecaster = new DemandForecaster(spark, testTrainingConfig())

      // Labels are the trip count plus one, so the persistence baseline is
      // wrong by exactly one trip on every row.
      val frame = spark
        .createDataFrame(
          Seq(
            (1, BaseSlot, 10.0, 11.0),
            (1, BaseSlot + SlideMillis, 20.0, 21.0),
            (1, BaseSlot + 2 * SlideMillis, 30.0, 31.0)
          )
        )
        .toDF(
          FeatureColumns.LocationId,
          FeatureColumns.WindowStart,
          FeatureColumns.TripCount,
          FeatureColumns.Target
        )

      val metrics = forecaster.baselineMetrics(frame)
      metrics.rowCount shouldBe 3L
      metrics.rmse shouldBe 1.0 +- Tolerance
      metrics.mae shouldBe 1.0 +- Tolerance
      metrics.bias shouldBe -1.0 +- Tolerance
    }

    it("prefers the daily seasonal lag when the matrix carries one") {
      val forecaster = new DemandForecaster(spark, testTrainingConfig())

      val frame = spark
        .createDataFrame(
          Seq(
            (1, BaseSlot, 10.0, 50.0, 50.0),
            (1, BaseSlot + SlideMillis, 20.0, 60.0, 60.0)
          )
        )
        .toDF(
          FeatureColumns.LocationId,
          FeatureColumns.WindowStart,
          FeatureColumns.TripCount,
          s"${FeatureColumns.TripCount}_lag_day",
          FeatureColumns.Target
        )

      // The daily lag matches the label exactly; persistence would not.
      forecaster.baselineMetrics(frame).rmse shouldBe 0.0 +- Tolerance
    }
  }

  describe("DemandForecaster.skillScore") {

    it("expresses the fraction of baseline error removed") {
      val forecaster = new DemandForecaster(spark, testTrainingConfig())
      val model = ForecastMetrics(10L, 4.0, 3.0, 0.8, 10.0, 1.0, 20.0, 20.0)
      val baseline = ForecastMetrics(10L, 8.0, 6.0, 0.4, 20.0, 2.0, 20.0, 20.0)

      forecaster.skillScore(model, baseline) shouldBe 0.5 +- Tolerance
      forecaster.skillScore(baseline, baseline) shouldBe 0.0 +- Tolerance
      forecaster.skillScore(model, baseline.copy(rmse = 0.0)) shouldBe 0.0
    }
  }

  describe("DemandForecaster.train") {

    it("learns a signal the persistence baseline cannot capture") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val forecaster = new DemandForecaster(spark, testTrainingConfig())

      val matrix = learnableMatrix(spark, pipeline, config, zones = 3, slots = 400)
      val model = forecaster.train(matrix)

      model.trainingRows should be > 0L
      model.metrics.rmse should be < forecaster.baselineMetrics(matrix).rmse
    }

    it("never predicts a negative trip count") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val forecaster = new DemandForecaster(spark, testTrainingConfig())

      val matrix = learnableMatrix(spark, pipeline, config, zones = 3, slots = 300)
      val model = forecaster.train(matrix)
      val forecasts = forecaster.forecast(model, matrix)

      forecasts.filter(col(DemandForecaster.ForecastColumn) < 0.0).count() shouldBe 0L
    }

    it("rejects an empty training set rather than fitting on nothing") {
      val forecaster = new DemandForecaster(spark, testTrainingConfig())
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val empty = learnableMatrix(spark, pipeline, config, zones = 1, slots = 40).limit(0)

      a[com.taxi.analytics.ml.ForecastException] should be thrownBy forecaster.train(empty)
    }

    it("exposes one named importance per assembled feature") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val forecaster = new DemandForecaster(spark, testTrainingConfig())

      val matrix = learnableMatrix(spark, pipeline, config, zones = 2, slots = 200)
      val model = forecaster.train(matrix)
      val importances = forecaster.featureImportances(model)

      importances should have size model.featureNames.size
      importances.map(_.importance).sum shouldBe 1.0 +- 0.01
      importances.head.rank shouldBe 1
    }
  }

  describe("DemandForecaster.forecast") {

    it("brackets every point estimate with its prediction interval") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val forecaster = new DemandForecaster(spark, testTrainingConfig())

      val matrix = learnableMatrix(spark, pipeline, config, zones = 2, slots = 250)
      val model = forecaster.train(matrix)
      val forecasts = forecaster.forecast(model, matrix)

      forecasts.columns should contain(DemandForecaster.ForecastLowerColumn)
      forecasts.columns should contain(DemandForecaster.ForecastUpperColumn)

      val inverted = forecasts
        .filter(
          col(DemandForecaster.ForecastLowerColumn) > col(DemandForecaster.ForecastUpperColumn) ||
            col(DemandForecaster.ForecastLowerColumn) < 0.0
        )
        .count()

      inverted shouldBe 0L
    }
  }

  describe("DemandForecaster.walkForwardValidate") {

    it("keeps every validation block a full embargo behind its training cutoff") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val trainingConfig = testTrainingConfig()
      val forecaster = new DemandForecaster(spark, trainingConfig)

      val matrix = learnableMatrix(spark, pipeline, config, zones = 2, slots = 500)
      val report = forecaster.walkForwardValidate(matrix, folds = 2)

      report.folds should have size 2
      report.folds.foreach { fold =>
        (fold.validationStart - fold.trainCutoff) shouldBe trainingConfig.embargoMillis
        fold.trainRows should be > 0L
        fold.validationRows should be > 0L
      }
    }

    it("expands the training range with each successive fold") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val forecaster = new DemandForecaster(spark, testTrainingConfig())

      val matrix = learnableMatrix(spark, pipeline, config, zones = 2, slots = 500)
      val report = forecaster.walkForwardValidate(matrix, folds = 3)

      val cutoffs = report.folds.map(_.trainCutoff)
      cutoffs shouldBe cutoffs.sorted
      report.folds.map(_.trainRows) shouldBe report.folds.map(_.trainRows).sorted
    }

    it("refuses a fold layout whose blocks are shorter than the embargo") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val forecaster = new DemandForecaster(spark, testTrainingConfig().copy(embargoMillis = Long.MaxValue / 4))

      val matrix = learnableMatrix(spark, pipeline, config, zones = 1, slots = 60)
      a[com.taxi.analytics.ml.ForecastException] should be thrownBy forecaster.walkForwardValidate(matrix, folds = 2)
    }
  }

  describe("DemandForecaster persistence") {

    it("round-trips a model with its feature order and metrics intact") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val forecaster = new DemandForecaster(spark, testTrainingConfig())

      val matrix = learnableMatrix(spark, pipeline, config, zones = 2, slots = 200)
      val model = forecaster.train(matrix)

      val path = tempRoot.resolve("model-roundtrip").toString
      forecaster.save(model, path, modelVersion = "spec-version", skillScore = 0.25)

      val reloaded = forecaster.load(path)
      reloaded.featureNames shouldBe model.featureNames
      reloaded.trainingRows shouldBe model.trainingRows
      reloaded.metrics.rmse shouldBe model.metrics.rmse +- Tolerance
      reloaded.family shouldBe ModelFamily.GradientBoosted
    }

    it("rejects an empty model version") {
      val config = testFeatureConfig()
      val pipeline = new FeaturePipeline(spark, config)
      val forecaster = new DemandForecaster(spark, testTrainingConfig())
      val model = forecaster.train(learnableMatrix(spark, pipeline, config, zones = 1, slots = 100))

      an[IllegalArgumentException] should be thrownBy forecaster.save(
        model,
        tempRoot.resolve("model-rejected").toString,
        modelVersion = "  "
      )
    }
  }

  // =========================================================================
  // Tier 3: integration against a live graph
  // =========================================================================

  describe("Neo4jConnector") {

    it("reaches the configured database", Neo4jIntegration) {
      val config = integrationConfig()
      assume(config.isDefined, "NEO4J_TEST_URI and NEO4J_TEST_PASSWORD are not set")

      Neo4jConnector.using(config.get) { connector =>
        connector.ping() shouldBe true
      }
    }

    it("round-trips a batch write through Cypher", Neo4jIntegration) {
      val config = integrationConfig()
      assume(config.isDefined, "NEO4J_TEST_URI and NEO4J_TEST_PASSWORD are not set")

      Neo4jConnector.using(config.get) { connector =>
        val label = s"SpecNode${System.nanoTime()}"

        try {
          val outcome = connector.writeBatch(
            s"UNWIND $$rows AS row MERGE (n:$label {key: row.key}) SET n.value = row.value",
            Iterator(Map[String, Any]("key" -> 1, "value" -> 10.0), Map[String, Any]("key" -> 2, "value" -> 20.0))
          )

          outcome.rowsSubmitted shouldBe 2L

          val values = connector.read(s"MATCH (n:$label) RETURN n.value AS value ORDER BY value")(
            record => record.get("value").asDouble(0.0)
          )

          values shouldBe Vector(10.0, 20.0)
        } finally {
          val _ = connector.write(s"MATCH (n:$label) DETACH DELETE n")
        }
      }
    }

    it("does not retry a syntax error", Neo4jIntegration) {
      val config = integrationConfig()
      assume(config.isDefined, "NEO4J_TEST_URI and NEO4J_TEST_PASSWORD are not set")

      Neo4jConnector.using(config.get) { connector =>
        // A client error is not retryable: repeating it multiplies load without
        // changing the outcome, so it must surface immediately.
        a[com.taxi.analytics.database.Neo4jExecutionException] should be thrownBy connector.write(
          "MATCH (n RETURN n"
        )
      }
    }
  }

  describe("GraphProjectionBuilder") {

    it("reports a projection that was never created as absent", Neo4jIntegration) {
      val config = integrationConfig()
      assume(config.isDefined, "NEO4J_TEST_URI and NEO4J_TEST_PASSWORD are not set")

      Neo4jConnector.using(config.get) { connector =>
        val builder = new GraphProjectionBuilder(connector)
        val name = s"spec_absent_${System.nanoTime()}"

        builder.exists(name) shouldBe false
        builder.drop(name) shouldBe false
        builder.describe(name) shouldBe None
      }
    }

    it("finds no generations for an unknown logical name", Neo4jIntegration) {
      val config = integrationConfig()
      assume(config.isDefined, "NEO4J_TEST_URI and NEO4J_TEST_PASSWORD are not set")

      Neo4jConnector.using(config.get) { connector =>
        val builder = new GraphProjectionBuilder(connector)
        builder.generationsOf(s"spec_unknown_${System.nanoTime()}") shouldBe empty
        builder.activeGraphName(s"spec_unknown_${System.nanoTime()}") shouldBe None
      }
    }
  }

  // =========================================================================
  // Fixtures
  // =========================================================================

  private def writeTempFile(name: String, content: String): Path = {
    val target = tempRoot.resolve(name)
    Files.write(target, content.getBytes(StandardCharsets.UTF_8))
    target
  }

  private def deleteRecursively(path: Path): Unit =
    try {
      if (Files.isDirectory(path)) {
        val stream = Files.list(path)
        try {
          val children = stream.toArray.map(_.asInstanceOf[Path])
          children.foreach(deleteRecursively)
        } finally {
          stream.close()
        }
      }
      Files.deleteIfExists(path)
      ()
    } catch {
      case NonFatal(_) => ()
    }
}

object PipelineSpec {

  private val Tolerance = 1e-6

  /** Slide interval shared by every synthetic fixture, in milliseconds. */
  private val SlideMillis = 300000L

  /**
   * Anchor slot for synthetic data, expressed as a plain epoch offset rather
   * than a calendar instant so the fixtures carry no embedded calendar
   * assumption.
   */
  private val BaseSlot = 1_600_000_000_000L

  private def testNeo4jConfig(uri: String, user: String): Neo4jConfig = Neo4jConfig(
    uri = uri,
    user = user,
    password = "secret",
    database = "neo4j",
    maxConnectionPoolSize = 8,
    connectionAcquisitionTimeoutSeconds = 30,
    connectionTimeoutSeconds = 30,
    maxConnectionLifetimeMinutes = 60,
    connectionLivenessCheckSeconds = 60,
    maxTransactionRetrySeconds = 30,
    fetchSize = 500,
    writeBatchSize = 500,
    userAgent = "pipeline-spec",
    encrypted = false,
    driverMetricsEnabled = false,
    trackBookmarks = false,
    retryPolicy = RetryPolicy.Default
  )

  /** Integration credentials, absent unless the harness supplied them. */
  private def integrationConfig(): Option[Neo4jConfig] =
    for {
      uri <- sys.env.get("NEO4J_TEST_URI").map(_.trim).filter(_.nonEmpty)
      password <- sys.env.get("NEO4J_TEST_PASSWORD").map(_.trim).filter(_.nonEmpty)
    } yield testNeo4jConfig(uri, sys.env.getOrElse("NEO4J_TEST_USER", "neo4j")).copy(
      password = password,
      database = sys.env.getOrElse("NEO4J_TEST_DATABASE", "neo4j")
    )

  /**
   * Feature configuration for the transform tests: short lags and no seasonal
   * terms, so a few hundred synthetic slots survive the warm-up trim.
   */
  private def testFeatureConfig(): FeatureConfig = FeatureConfig(
    neo4jUri = "bolt://unused:7687",
    neo4jUser = "neo4j",
    neo4jPassword = "unused",
    neo4jDatabase = "neo4j",
    slideSeconds = 300,
    windowSeconds = 900,
    horizonSlots = 3,
    lagSlots = Seq(1, 2, 3),
    rollingSlots = Seq(6, 12),
    includeDailySeasonalLag = false,
    includeWeeklySeasonalLag = false,
    includeFlowFeatures = false,
    graphFeatureSource = GraphFeatureSource.Disabled,
    displayTimeZone = "America/New_York",
    minObservedSlots = 1,
    readPartitions = 2,
    scaleFeatures = false
  )

  /** Small, fast ensemble so the suite stays usable in a pre-commit hook. */
  private def testTrainingConfig(): TrainingConfig = TrainingConfig(
    family = ModelFamily.GradientBoosted,
    gbt = GbtParams(maxIter = 20, maxDepth = 4, stepSize = 0.2),
    embargoMillis = 3 * SlideMillis,
    seed = 7L
  )

  private def testFold(index: Int, modelRmse: Double, baselineRmse: Double): FoldResult = FoldResult(
    foldIndex = index,
    trainRows = 100L * index,
    validationRows = 50L,
    trainCutoff = BaseSlot + index.toLong * SlideMillis,
    validationStart = BaseSlot + (index.toLong + 1L) * SlideMillis,
    metrics = ForecastMetrics(50L, modelRmse, modelRmse * 0.75, 0.5, 15.0, 1.0, 20.0, 20.0),
    baseline = ForecastMetrics(50L, baselineRmse, baselineRmse * 0.75, 0.2, 25.0, 2.0, 20.0, 20.0)
  )

  private def zoneImportance(normalisedPageRank: Double, normalisedBetweenness: Double): ZoneImportance =
    ZoneImportance(
      locationId = 132,
      zoneName = "Test Zone",
      borough = "Queens",
      pageRank = 0.01,
      betweenness = 500.0,
      normalisedPageRank = normalisedPageRank,
      normalisedBetweenness = normalisedBetweenness,
      importanceScore = 0.5,
      importanceRank = 1
    )

  private def testTripEvent(pickupZone: Int, dropoffZone: Int): TripEvent = {
    val pickup = Instant.ofEpochMilli(BaseSlot)
    val dropoff = Instant.ofEpochMilli(BaseSlot + 600000L)

    TripEvent(
      eventId = "00000000-0000-0000-0000-000000000000",
      serviceType = "yellow",
      vendorId = None,
      pickupTime = pickup,
      dropoffTime = dropoff,
      tripDurationSeconds = 600L,
      passengerCount = Some(2),
      tripDistanceMiles = 3.2,
      averageSpeedMph = Some(19.2),
      pickupLocationId = pickupZone,
      dropoffLocationId = dropoffZone,
      rateCodeId = Some(1),
      paymentType = Some(1),
      storeAndForwardFlag = Some("N"),
      fareAmount = Some(14.5),
      extraAmount = Some(0.5),
      mtaTax = Some(0.5),
      tipAmount = Some(3.0),
      tollsAmount = Some(0.0),
      improvementSurcharge = Some(0.3),
      congestionSurcharge = Some(2.5),
      airportFee = None,
      ehailFee = None,
      totalAmount = 21.3,
      sourceFile = "spec.csv",
      sourceLine = 2L,
      ingestedAt = pickup
    )
  }

  /** Column layout matching what FeaturePipeline.loadDemandWindows produces. */
  private val DemandSchema: StructType = StructType(
    Seq(
      StructField(FeatureColumns.LocationId, IntegerType, nullable = false),
      StructField(FeatureColumns.WindowStart, LongType, nullable = false),
      StructField(FeatureColumns.WindowEnd, LongType, nullable = false),
      StructField(FeatureColumns.TripCount, DoubleType, nullable = true),
      StructField(FeatureColumns.PassengerTotal, DoubleType, nullable = true),
      StructField(FeatureColumns.DemandPerMinute, DoubleType, nullable = true),
      StructField(FeatureColumns.AvgTripDistance, DoubleType, nullable = true),
      StructField(FeatureColumns.AvgDuration, DoubleType, nullable = true),
      StructField(FeatureColumns.AvgSpeed, DoubleType, nullable = true),
      StructField(FeatureColumns.AvgFare, DoubleType, nullable = true),
      StructField(FeatureColumns.TotalRevenue, DoubleType, nullable = true),
      StructField(FeatureColumns.TotalTips, DoubleType, nullable = true),
      StructField(FeatureColumns.DistinctDestinations, DoubleType, nullable = true),
      StructField(FeatureColumns.SurgeRatio, DoubleType, nullable = true),
      StructField(FeatureColumns.BaselineDemand, DoubleType, nullable = true),
      StructField(FeatureColumns.IsSurge, DoubleType, nullable = true)
    )
  )

  private def demandRow(
      locationId: Int,
      slotIndex: Int,
      tripCount: Double,
      avgSpeed: Double
  ): Row = {
    val slot = BaseSlot + slotIndex.toLong * SlideMillis
    Row(
      locationId,
      slot,
      slot + 900000L,
      tripCount,
      tripCount * 1.4,
      tripCount / 15.0,
      2.5,
      720.0,
      avgSpeed,
      15.0,
      tripCount * 18.0,
      tripCount * 2.5,
      math.min(tripCount, 20.0),
      1.0,
      tripCount / 15.0,
      0.0
    )
  }

  private def buildDemandFrame(spark: SparkSession, rows: Seq[Row]): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), DemandSchema)

  /**
   * Two zones over six slots with deliberate gaps: zone 1 is missing slots 2
   * and 4, zone 2 appears only twice.
   */
  private def sparseDemandFrame(spark: SparkSession): DataFrame = {
    val zoneOne = Seq(0, 1, 3, 5).map(index => demandRow(1, index, 10.0 + index, 12.0))
    val zoneTwo = Seq(0, 5).map(index => demandRow(2, index, 4.0 + index, 9.0))
    buildDemandFrame(spark, zoneOne ++ zoneTwo)
  }

  private def constantDemandFrame(
      spark: SparkSession,
      zones: Int,
      slots: Int,
      tripCount: Double
  ): DataFrame = {
    val rows = for {
      zone <- 1 to zones
      slot <- 0 until slots
    } yield demandRow(zone, slot, tripCount, 12.0)
    buildDemandFrame(spark, rows)
  }

  /**
   * A dense matrix with a deterministic daily shape, used wherever a test
   * needs realistic column layout without caring about the values.
   */
  private def syntheticMatrix(
      spark: SparkSession,
      config: FeatureConfig,
      zones: Int,
      slots: Int
  ): DataFrame = {
    val rows = for {
      zone <- 1 to zones
      slot <- 0 until slots
    } yield {
      val phase = 2.0 * math.Pi * (slot.toDouble / config.slotsPerDay.toDouble)
      val level = 40.0 + 25.0 * math.sin(phase) + zone.toDouble * 5.0
      demandRow(zone, slot, math.max(0.0, level), 12.0)
    }
    buildDemandFrame(spark, rows)
  }

  /**
   * A matrix whose target is genuinely predictable from its lags, so a model
   * that beats the persistence baseline on it has learned something rather
   * than memorised noise.
   */
  private def learnableMatrix(
      spark: SparkSession,
      pipeline: FeaturePipeline,
      config: FeatureConfig,
      zones: Int,
      slots: Int
  ): DataFrame = {
    val raw = syntheticMatrix(spark, config, zones, slots)
    val calendar = pipeline.withCalendarFeatures(raw)
    val lagged = pipeline.withLagFeatures(calendar)
    val rolled = pipeline.withRollingFeatures(lagged)
    pipeline.trimWarmUp(pipeline.withTarget(rolled))
  }
}
