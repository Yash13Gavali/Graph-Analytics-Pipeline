package com.taxi.analytics

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.{CountDownLatch, Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}

import scala.collection.mutable
import scala.util.control.NonFatal

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import com.taxi.analytics.database.{Neo4jConfig, Neo4jConnector, RetryPolicy}
import com.taxi.analytics.graph.{
  BetweennessConfig,
  CentralityAnalytics,
  ClusterAnalytics,
  GraphProjectionBuilder,
  ImportanceWeights,
  LouvainConfig,
  PageRankConfig,
  ProjectionSummary
}
import com.taxi.analytics.ml.{
  DemandForecaster,
  FeatureColumns,
  FeatureConfig,
  FeaturePipeline,
  GraphFeatureSource,
  TrainedModel,
  TrainingConfig
}
import com.taxi.analytics.processor.{ProcessorSettings, StreamProcessor}

// ---------------------------------------------------------------------------
// Real-Time Graph Analytics & NYC Taxi Demand Forecasting
// Orchestration driver
//
// The system has three loops running at different cadences, and the driver
// exists to keep them from interfering:
//
//   Continuous  — Structured Streaming consumes trip events and writes demand
//                 windows and flow edges. Never stops.
//   Periodic    — the GDS projection is rebuilt and centrality and community
//                 passes rerun. Minutes, not seconds: rebuilding a projection
//                 on every micro-batch would spend the cluster on graph
//                 construction and starve the stream.
//   Periodic    — features are assembled from the newest windows and scored
//                 against the current model.
//
// Each cycle is independent and idempotent. A failed graph refresh leaves the
// previous generation serving; a failed inference cycle leaves the previous
// forecasts in place. Only repeated failure escalates.
// ---------------------------------------------------------------------------

/** What the driver was asked to run. */
sealed abstract class DriverMode(val name: String) extends Serializable

object DriverMode {

  /** Streaming ingestion only. Blocks until a query terminates. */
  case object Stream extends DriverMode("stream")

  /** One projection refresh with centrality and community passes. */
  case object Graph extends DriverMode("graph")

  /** One feature build, validation run and model fit. */
  case object Train extends DriverMode("train")

  /** One feature build and scoring pass against the saved model. */
  case object Infer extends DriverMode("infer")

  /** Streaming plus scheduled graph and inference cycles in one process. */
  case object All extends DriverMode("all")

  val Modes: Seq[DriverMode] = Seq(Stream, Graph, Train, Infer, All)

  def fromString(raw: String): Either[String, DriverMode] = {
    val normalized = raw.trim.toLowerCase
    Modes.find(_.name == normalized).toRight(s"Unknown mode '$raw'; expected ${Modes.map(_.name).mkString(", ")}")
  }
}

/** Full driver configuration. */
final case class DriverSettings(
    mode: DriverMode,
    appName: String,
    bootstrapServers: String,
    topic: String,
    startingOffsets: String,
    checkpointRoot: String,
    windowDuration: String,
    slideDuration: String,
    watermarkDelay: String,
    triggerIntervalSeconds: Int,
    shufflePartitions: Int,
    neo4jUri: String,
    neo4jUser: String,
    neo4jPassword: String,
    neo4jDatabase: String,
    graphRefreshMinutes: Int,
    inferenceMinutes: Int,
    projectionLookbackHours: Int,
    featureStorePath: String,
    graphSnapshotPath: String,
    modelPath: String,
    forecastPath: String,
    modelVersion: String,
    slotsToScore: Int,
    horizonSlots: Int,
    trainWindowFrom: Option[Long],
    trainWindowTo: Option[Long],
    validationFolds: Int,
    useGraphSnapshots: Boolean,
    publishForecastsToGraph: Boolean,
    maxConsecutiveFailures: Int
) {

  val slideSeconds: Int = IntervalSeconds.parse(slideDuration)
  val windowSeconds: Int = IntervalSeconds.parse(windowDuration)
  val slideMillis: Long = slideSeconds.toLong * 1000L

  def neo4jConfig: Neo4jConfig = Neo4jConfig(
    uri = neo4jUri,
    user = neo4jUser,
    password = neo4jPassword,
    database = neo4jDatabase,
    maxConnectionPoolSize = 32,
    connectionAcquisitionTimeoutSeconds = 60,
    connectionTimeoutSeconds = 30,
    maxConnectionLifetimeMinutes = 60,
    connectionLivenessCheckSeconds = 60,
    maxTransactionRetrySeconds = 30,
    fetchSize = 1000,
    writeBatchSize = 2000,
    userAgent = "nyc-taxi-graph-analytics",
    encrypted = false,
    driverMetricsEnabled = true,
    trackBookmarks = true,
    retryPolicy = RetryPolicy.Default
  )

  def featureConfig: FeatureConfig = FeatureConfig(
    neo4jUri = neo4jUri,
    neo4jUser = neo4jUser,
    neo4jPassword = neo4jPassword,
    neo4jDatabase = neo4jDatabase,
    slideSeconds = slideSeconds,
    windowSeconds = windowSeconds,
    horizonSlots = horizonSlots,
    lagSlots = Seq(1, 2, 3, 6, 12),
    rollingSlots = Seq(6, 12, 36),
    includeDailySeasonalLag = 86400 % slideSeconds == 0,
    includeWeeklySeasonalLag = 86400 % slideSeconds == 0,
    includeFlowFeatures = true,
    graphFeatureSource =
      if (useGraphSnapshots) GraphFeatureSource.Snapshots(graphSnapshotPath)
      else GraphFeatureSource.CurrentNodeProperties,
    displayTimeZone = "America/New_York",
    minObservedSlots = 1,
    readPartitions = 8,
    scaleFeatures = false
  )

  def trainingConfig: TrainingConfig = TrainingConfig(
    embargoMillis = horizonSlots.toLong * slideMillis
  )

  /** Arguments forwarded verbatim to the streaming job's own parser. */
  def streamingArguments: Array[String] = Array(
    "--app-name", appName,
    "--bootstrap-servers", bootstrapServers,
    "--topic", topic,
    "--starting-offsets", startingOffsets,
    "--checkpoint-location", checkpointRoot,
    "--window-duration", windowDuration,
    "--slide-duration", slideDuration,
    "--watermark-delay", watermarkDelay,
    "--trigger-interval", triggerIntervalSeconds.toString,
    "--shuffle-partitions", shufflePartitions.toString,
    "--neo4j-uri", neo4jUri,
    "--neo4j-user", neo4jUser,
    "--neo4j-database", neo4jDatabase
  )

  def describe: String =
    s"mode=${mode.name} topic=$topic window=$windowDuration slide=$slideDuration " +
      s"graph_refresh=${graphRefreshMinutes}m inference=${inferenceMinutes}m horizon=$horizonSlots"
}

/** Parses Spark-style interval strings into seconds. */
private[analytics] object IntervalSeconds {

  private val Pattern = """^\s*(\d+)\s+(second|seconds|minute|minutes|hour|hours)\s*$""".r

  def isValid(spec: String): Boolean = Pattern.findFirstIn(spec).isDefined

  def parse(spec: String): Int = spec match {
    case Pattern(amount, unit) =>
      val magnitude = amount.toInt
      unit match {
        case "second" | "seconds" => magnitude
        case "minute" | "minutes" => magnitude * 60
        case _ => magnitude * 3600
      }
    case _ =>
      throw new IllegalArgumentException(s"Unsupported interval specification: $spec")
  }
}

object DriverSettings {

  val Usage: String =
    """
      |Usage: PipelineDriver --mode <mode> [options]
      |
      |Modes:
      |  stream   Streaming ingestion only; blocks until a query terminates.
      |  graph    One projection refresh with centrality and community passes.
      |  train    One feature build, walk-forward validation and model fit.
      |  infer    One feature build and scoring pass against the saved model.
      |  all      Streaming plus scheduled graph and inference cycles.
      |
      |Kafka source:
      |  --bootstrap-servers <list>     Kafka bootstrap servers        (default: kafka:9092)
      |  --topic <name>                 Source topic                   (default: nyc-taxi-trips)
      |  --starting-offsets <spec>      earliest | latest | JSON spec  (default: latest)
      |
      |Windowing:
      |  --window-duration <spec>       Sliding window width           (default: 15 minutes)
      |  --slide-duration <spec>        Slide interval                 (default: 5 minutes)
      |  --watermark-delay <spec>       Allowed event-time lateness    (default: 10 minutes)
      |  --trigger-interval <seconds>   Micro-batch cadence            (default: 30)
      |  --horizon-slots <n>            Forecast horizon in slots      (default: 3)
      |
      |Cadence:
      |  --graph-refresh-minutes <n>    Projection refresh interval    (default: 15)
      |  --inference-minutes <n>        Scoring interval               (default: 5)
      |  --projection-lookback-hours <n> Flow history per projection   (default: 24)
      |  --slots-to-score <n>           Slots scored per cycle         (default: 1)
      |
      |Paths:
      |  --checkpoint-location <path>   Streaming checkpoint root      (default: /opt/spark/checkpoints)
      |  --feature-store <path>         Feature store root             (default: /opt/spark/data/features)
      |  --graph-snapshot-path <path>   Graph feature snapshots        (default: /opt/spark/data/graph-snapshots)
      |  --model-path <path>            Model directory                (default: /opt/spark/data/models/demand)
      |  --forecast-path <path>         Forecast output root           (default: /opt/spark/data/forecasts)
      |
      |Training:
      |  --model-version <id>           Version label for a saved model (default: current)
      |  --train-window-from <millis>   Lower event-time bound
      |  --train-window-to <millis>     Upper event-time bound
      |  --validation-folds <n>         Walk-forward folds             (default: 4)
      |  --use-graph-snapshots          Join point-in-time graph snapshots instead of current properties.
      |
      |Execution:
      |  --app-name <name>              Spark application name         (default: nyc-taxi-pipeline)
      |  --shuffle-partitions <n>       Shuffle parallelism            (default: 16)
      |  --max-consecutive-failures <n> Cycle failures before abort    (default: 5)
      |  --publish-forecasts-to-graph   Also write forecasts back to Neo4j.
      |
      |Neo4j:
      |  --neo4j-uri <uri>              Bolt URI                       (default: bolt://neo4j:7687)
      |  --neo4j-user <user>            Username                       (default: neo4j)
      |  --neo4j-database <name>        Target database                (default: neo4j)
      |  The password is read from NEO4J_PASSWORD only.
      |
      |  --help                         Print this message.
      |""".stripMargin

  def parse(args: Array[String]): Either[String, DriverSettings] = {
    val parsed = mutable.LinkedHashMap.empty[String, String]
    val flags = mutable.Set.empty[String]
    val booleanFlags = Set("help", "use-graph-snapshots", "publish-forecasts-to-graph")

    var index = 0
    while (index < args.length) {
      val token = args(index)
      if (!token.startsWith("--")) {
        return Left(s"Unexpected positional argument: $token")
      }
      val key = token.substring(2)
      if (booleanFlags.contains(key)) {
        flags += key
        index += 1
      } else {
        if (index + 1 >= args.length) {
          return Left(s"Missing value for --$key")
        }
        parsed.put(key, args(index + 1))
        index += 2
      }
    }

    if (flags.contains("help")) {
      return Left(Usage)
    }

    def optional(key: String): Option[String] = parsed.get(key).map(_.trim).filter(_.nonEmpty)

    def intOption(key: String, default: Int, minimum: Int): Either[String, Int] =
      optional(key) match {
        case None => Right(default)
        case Some(raw) =>
          try {
            val value = raw.toInt
            if (value < minimum) Left(s"--$key must be >= $minimum") else Right(value)
          } catch { case _: NumberFormatException => Left(s"--$key expects an integer, got '$raw'") }
      }

    def longOption(key: String): Either[String, Option[Long]] =
      optional(key) match {
        case None => Right(None)
        case Some(raw) =>
          try Right(Some(raw.toLong))
          catch { case _: NumberFormatException => Left(s"--$key expects an integer, got '$raw'") }
      }

    def intervalOption(key: String, default: String): Either[String, String] = {
      val value = optional(key).getOrElse(default)
      if (IntervalSeconds.isValid(value)) Right(value)
      else Left(s"--$key expects an interval such as '15 minutes', got '$value'")
    }

    for {
      rawMode <- optional("mode").toRight("--mode is required")
      mode <- DriverMode.fromString(rawMode)
      password <- Option(System.getenv("NEO4J_PASSWORD"))
        .map(_.trim)
        .filter(_.nonEmpty)
        .toRight("NEO4J_PASSWORD must be set")
      windowDuration <- intervalOption("window-duration", "15 minutes")
      slideDuration <- intervalOption("slide-duration", "5 minutes")
      watermarkDelay <- intervalOption("watermark-delay", "10 minutes")
      _ <- if (IntervalSeconds.parse(slideDuration) <= IntervalSeconds.parse(windowDuration)) Right(())
           else Left("--slide-duration must not exceed --window-duration")
      triggerInterval <- intOption("trigger-interval", 30, 1)
      shufflePartitions <- intOption("shuffle-partitions", 16, 1)
      graphRefresh <- intOption("graph-refresh-minutes", 15, 1)
      inference <- intOption("inference-minutes", 5, 1)
      lookbackHours <- intOption("projection-lookback-hours", 24, 1)
      horizonSlots <- intOption("horizon-slots", 3, 1)
      slotsToScore <- intOption("slots-to-score", 1, 1)
      validationFolds <- intOption("validation-folds", 4, 1)
      maxFailures <- intOption("max-consecutive-failures", 5, 1)
      trainFrom <- longOption("train-window-from")
      trainTo <- longOption("train-window-to")
      _ <- (trainFrom, trainTo) match {
        case (Some(from), Some(to)) if from >= to => Left("--train-window-from must be less than --train-window-to")
        case _ => Right(())
      }
    } yield DriverSettings(
      mode = mode,
      appName = optional("app-name").getOrElse("nyc-taxi-pipeline"),
      bootstrapServers = optional("bootstrap-servers").getOrElse("kafka:9092"),
      topic = optional("topic").getOrElse("nyc-taxi-trips"),
      startingOffsets = optional("starting-offsets").getOrElse("latest"),
      checkpointRoot = optional("checkpoint-location").getOrElse("/opt/spark/checkpoints"),
      windowDuration = windowDuration,
      slideDuration = slideDuration,
      watermarkDelay = watermarkDelay,
      triggerIntervalSeconds = triggerInterval,
      shufflePartitions = shufflePartitions,
      neo4jUri = optional("neo4j-uri").getOrElse("bolt://neo4j:7687"),
      neo4jUser = optional("neo4j-user").getOrElse("neo4j"),
      neo4jPassword = password,
      neo4jDatabase = optional("neo4j-database").getOrElse("neo4j"),
      graphRefreshMinutes = graphRefresh,
      inferenceMinutes = inference,
      projectionLookbackHours = lookbackHours,
      featureStorePath = optional("feature-store").getOrElse("/opt/spark/data/features"),
      graphSnapshotPath = optional("graph-snapshot-path").getOrElse("/opt/spark/data/graph-snapshots"),
      modelPath = optional("model-path").getOrElse("/opt/spark/data/models/demand"),
      forecastPath = optional("forecast-path").getOrElse("/opt/spark/data/forecasts"),
      modelVersion = optional("model-version").getOrElse("current"),
      slotsToScore = slotsToScore,
      horizonSlots = horizonSlots,
      trainWindowFrom = trainFrom,
      trainWindowTo = trainTo,
      validationFolds = validationFolds,
      useGraphSnapshots = flags.contains("use-graph-snapshots"),
      publishForecastsToGraph = flags.contains("publish-forecasts-to-graph"),
      maxConsecutiveFailures = maxFailures
    )
  }
}

/** Raised when the driver cannot continue. */
final class PipelineException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

/**
 * Coordinates the streaming, graph and forecasting stages.
 *
 * The driver owns the periodic cycles but deliberately does not own the
 * streaming session: `StreamProcessor` builds and stops its own, so in `all`
 * mode the stream is supervised on its own thread and its termination is
 * treated as a shutdown signal for the whole process. That is the correct
 * coupling — once ingestion stops, every downstream cycle is scoring stale
 * windows and should not keep running as though nothing happened.
 */
final class PipelineDriver(settings: DriverSettings) extends StrictLogging {

  import PipelineDriver._

  private val connector = Neo4jConnector.shared(settings.neo4jConfig)
  private val projections = new GraphProjectionBuilder(connector)
  private val centrality = new CentralityAnalytics(connector)
  private val clustering = new ClusterAnalytics(connector)

  private val running = new AtomicBoolean(true)
  private val terminated = new CountDownLatch(1)
  private val cachedModel = new AtomicReference[Option[LoadedModel]](None)
  private val graphFailures = new AtomicInteger(0)
  private val inferenceFailures = new AtomicInteger(0)
  private val graphCycles = new AtomicInteger(0)
  private val inferenceCycles = new AtomicInteger(0)

  def run(): Int = {
    logger.info(s"Pipeline driver starting: ${settings.describe}")
    preflight()

    settings.mode match {
      case DriverMode.Stream => runStreaming()
      case DriverMode.Graph => runOnce("graph refresh")(graphCycle())
      case DriverMode.Train => runOnce("training")(trainingCycle())
      case DriverMode.Infer => runOnce("inference")(inferenceCycle())
      case DriverMode.All => runAll()
    }
  }

  // -------------------------------------------------------------------------
  // Preflight
  // -------------------------------------------------------------------------

  /**
   * Verifies the graph is reachable and that the schema bootstrap has run.
   * A missing uniqueness constraint does not fail a write, it silently allows
   * duplicates under concurrency, so it is worth naming loudly at startup.
   */
  private def preflight(): Unit = {
    connector.verifyConnectivity()

    val present = connector
      .read("SHOW CONSTRAINTS YIELD name RETURN name")(record => record.get("name").asString(""))
      .toSet

    val missing = RequiredConstraints.filterNot(present.contains)
    if (missing.nonEmpty) {
      logger.warn(
        s"Expected constraints are absent: ${missing.mkString(", ")}. " +
          "Apply conf/neo4j/schema.cypher before running in production; without them concurrent " +
          "MERGE can create duplicate nodes."
      )
    }
  }

  // -------------------------------------------------------------------------
  // Mode: stream
  // -------------------------------------------------------------------------

  private def runStreaming(): Int =
    ProcessorSettings.parse(settings.streamingArguments) match {
      case Left(message) =>
        logger.error(s"Streaming configuration rejected: $message")
        1
      case Right(processorSettings) =>
        try {
          new StreamProcessor(processorSettings).run()
          0
        } catch {
          case NonFatal(error) =>
            logger.error(s"Streaming stage failed: ${error.getMessage}", error)
            2
        }
    }

  // -------------------------------------------------------------------------
  // Mode: all
  // -------------------------------------------------------------------------

  /**
   * Runs the stream on a supervised thread and schedules the periodic cycles.
   *
   * `scheduleWithFixedDelay` measures the gap from the end of one run to the
   * start of the next, so a cycle that overruns its interval delays the next
   * rather than stacking a second copy on top of it.
   */
  private def runAll(): Int = {
    val scheduler = newScheduler()
    val streamThread = new Thread(new Runnable {
      override def run(): Unit = {
        val exit = runStreaming()
        logger.warn(s"Streaming stage exited with code $exit; signalling driver shutdown")
        requestShutdown()
      }
    }, "pipeline-streaming")
    streamThread.setDaemon(false)

    registerShutdownHook(scheduler)

    // The first graph refresh is delayed by one interval so the stream has
    // written enough windows for a projection to be non-degenerate.
    val _ = scheduler.scheduleWithFixedDelay(
      guarded("graph refresh", graphFailures)(graphCycle()),
      settings.graphRefreshMinutes.toLong,
      settings.graphRefreshMinutes.toLong,
      TimeUnit.MINUTES
    )

    val _ = scheduler.scheduleWithFixedDelay(
      guarded("inference", inferenceFailures)(inferenceCycle()),
      settings.inferenceMinutes.toLong,
      settings.inferenceMinutes.toLong,
      TimeUnit.MINUTES
    )

    val _ = scheduler.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = logStatus()
      },
      StatusIntervalMinutes,
      StatusIntervalMinutes,
      TimeUnit.MINUTES
    )

    streamThread.start()

    try {
      terminated.await()
      logger.info("Driver shutdown complete")
      0
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        2
    } finally {
      shutdownScheduler(scheduler)
      closeResources()
    }
  }

  /**
   * Wraps a cycle so that a single failure is logged and skipped, while a run
   * of consecutive failures escalates. One bad cycle is usually a transient
   * broker or database blip; five in a row is a real fault, and continuing to
   * publish forecasts from a pipeline in that state is worse than stopping.
   */
  private def guarded(label: String, failures: AtomicInteger)(body: => Unit): Runnable =
    new Runnable {
      override def run(): Unit =
        if (!running.get()) {
          logger.debug(s"Skipping $label cycle during shutdown")
        } else {
          try {
            body
            val previous = failures.getAndSet(0)
            if (previous > 0) {
              logger.info(s"$label cycle recovered after $previous consecutive failures")
            }
          } catch {
            case NonFatal(error) =>
              val consecutive = failures.incrementAndGet()
              logger.error(s"$label cycle failed ($consecutive consecutive): ${error.getMessage}", error)
              if (consecutive >= settings.maxConsecutiveFailures) {
                logger.error(s"$label exceeded ${settings.maxConsecutiveFailures} consecutive failures; stopping driver")
                requestShutdown()
              }
          }
        }
    }

  private def runOnce(label: String)(body: => Unit): Int =
    try {
      body
      logger.info(s"$label completed")
      0
    } catch {
      case NonFatal(error) =>
        logger.error(s"$label failed: ${error.getMessage}", error)
        2
    } finally {
      closeResources()
    }

  // -------------------------------------------------------------------------
  // Cycle: graph refresh
  // -------------------------------------------------------------------------

  /**
   * Rebuilds both projections over the trailing window and reruns the
   * analytics passes.
   *
   * Order matters: centrality writes `importanceScore`, which the community
   * profiler reads to pick the representative zones for each community. Both
   * complete before the snapshot is written so that the snapshot is internally
   * consistent.
   */
  def graphCycle(): Unit = {
    val latestWindow = latestWindowStart().getOrElse(
      throw new PipelineException("No demand windows found; the streaming stage has not produced output yet")
    )

    val windowFrom = latestWindow - (settings.projectionLookbackHours.toLong * 3600000L)
    val windowTo = latestWindow + settings.slideMillis

    logger.info(s"Graph cycle over [$windowFrom, $windowTo)")

    val flowProjection = refreshProjection(
      GraphProjectionBuilder.Specs.zoneFlowVolume(Some(windowFrom), Some(windowTo))
    )

    val ranked = centrality.analyseAndPersist(
      flowProjection,
      CentralityAnalytics.Presets.demandPull,
      CentralityAnalytics.Presets.exactCorridors,
      ImportanceWeights.Default
    )
    logger.info(s"Ranked ${ranked.size} zones by importance")

    ranked.take(TopZonesLogged).foreach { zone =>
      logger.info(f"rank=${zone.importanceRank}%d zone=${zone.zoneName}%s score=${zone.importanceScore}%.4f")
    }

    val communityProjection = refreshProjection(
      GraphProjectionBuilder.Specs.zoneCommunity(Some(windowFrom), Some(windowTo))
    )

    val (assignments, profiles, stability) = clustering.analyseAndPersist(
      communityProjection,
      ClusterAnalytics.Presets.catchments,
      Some(windowFrom),
      Some(windowTo)
    )
    logger.info(s"Detected ${profiles.size} communities across ${assignments.size} zones | ${stability.summary}")

    val crossBorough = clustering.crossBoroughCommunities(profiles)
    if (crossBorough.nonEmpty) {
      logger.info(s"${crossBorough.size} communities span multiple boroughs")
    }

    if (settings.useGraphSnapshots) {
      writeGraphSnapshot(windowTo)
    }

    val _ = graphCycles.incrementAndGet()
  }

  private def refreshProjection(spec: com.taxi.analytics.graph.ProjectionSpec): ProjectionSummary = {
    val cardinality = projections.sourceCardinality(spec)
    logger.info(s"Projection source: ${cardinality.summary}")

    if (cardinality.relationshipCount == 0L) {
      throw new PipelineException(
        s"No flow edges match ${spec.describe}; widen the lookback or lower the trip threshold"
      )
    }

    val summary = projections.refresh(spec)
    logger.info(s"Projection ready: ${summary.summary}")
    summary
  }

  private def writeGraphSnapshot(validFrom: Long): Unit = {
    val spark = activeSession()
    val pipeline = new FeaturePipeline(spark, settings.featureConfig)
    pipeline.writeGraphSnapshot(settings.graphSnapshotPath, validFrom)
  }

  // -------------------------------------------------------------------------
  // Cycle: training
  // -------------------------------------------------------------------------

  /**
   * Builds the training matrix, validates walk-forward, then refits on the
   * full range and saves.
   *
   * The model shipped is the one refit on everything, but the metrics recorded
   * against it come from validation. Reporting the in-sample fit of the final
   * model would be reporting a number nobody can reproduce in production.
   */
  def trainingCycle(): Unit = {
    val spark = activeSession()
    val featurePipeline = new FeaturePipeline(spark, settings.featureConfig)
    val forecaster = new DemandForecaster(spark, settings.trainingConfig)

    if (!settings.useGraphSnapshots) {
      logger.warn(
        "Training against current graph properties. Centrality reflects trips that had not occurred " +
          "at the time of the rows being labelled, so validation scores will be optimistic. " +
          "Run with --use-graph-snapshots once snapshots have accumulated."
      )
    }

    val matrix = featurePipeline
      .buildTrainingSet(settings.trainWindowFrom, settings.trainWindowTo)
      .cache()

    try {
      val rows = matrix.count()
      if (rows == 0L) {
        throw new PipelineException("Training matrix is empty; check the event-time bounds")
      }
      logger.info(s"Training matrix: $rows rows")
      featurePipeline.logFeatureHealth(matrix)

      featurePipeline.writeFeatureStore(matrix, settings.featureStorePath, SaveMode.Overwrite)

      val assembled = featurePipeline.assembleVector(matrix)
      val report = forecaster.walkForwardValidate(assembled, settings.validationFolds)

      if (!report.beatsBaseline) {
        logger.error(
          f"Model mean skill ${report.meanSkill}%+.4f does not beat the seasonal naive baseline. " +
            "Saving it anyway for inspection, but it should not be promoted."
        )
      }

      val model = forecaster.train(assembled)
      forecaster.logFeatureImportances(model)
      forecaster.save(model, settings.modelPath, settings.modelVersion, report.meanSkill)

      logger.info(s"Training complete: ${report.summary}")
    } finally {
      val _ = matrix.unpersist(blocking = false)
    }
  }

  // -------------------------------------------------------------------------
  // Cycle: inference
  // -------------------------------------------------------------------------

  /**
   * Scores the newest slots against the current model.
   *
   * The model is reloaded only when the saved version label changes, so a
   * retrain is picked up without a restart while an unchanged model is not
   * deserialised on every cycle.
   */
  def inferenceCycle(): Unit = {
    val spark = activeSession()
    val featurePipeline = new FeaturePipeline(spark, settings.featureConfig)
    val forecaster = new DemandForecaster(spark, settings.trainingConfig)

    val latestWindow = latestWindowStart().getOrElse(
      throw new PipelineException("No demand windows available to score")
    )

    val model = resolveModel(spark, forecaster)

    val inferenceMatrix = featurePipeline.buildInferenceSet(latestWindow, settings.slotsToScore)
    val assembled = featurePipeline.assembleVector(inferenceMatrix)
    val forecasts = forecaster.forecast(model.model, assembled).cache()

    try {
      val produced = forecasts.count()
      if (produced == 0L) {
        logger.warn("Inference produced no rows; the newest slots may be outside the retained history")
      } else {
        forecaster.writeForecasts(forecasts, settings.forecastPath, SaveMode.Append)

        if (settings.publishForecastsToGraph) {
          val published = publishForecasts(forecasts)
          logger.info(s"Published $published forecasts to the graph")
        }

        logger.info(s"Scored $produced zone-slots at window $latestWindow using model ${model.version}")
      }
    } finally {
      val _ = forecasts.unpersist(blocking = false)
    }

    val _ = inferenceCycles.incrementAndGet()
  }

  /**
   * Writes forecasts back onto the graph so the serving layer reads one store.
   * The result set is one row per zone per scored slot, so collecting it to the
   * driver is bounded by the zone dimension rather than by the trip volume.
   */
  private def publishForecasts(forecasts: DataFrame): Long = {
    val rows = forecasts
      .select(
        col(FeatureColumns.LocationId),
        col(FeatureColumns.WindowStart),
        col(DemandForecaster.ForecastColumn),
        col(DemandForecaster.ForecastLowerColumn),
        col(DemandForecaster.ForecastUpperColumn)
      )
      .collect()
      .toSeq
      .map { row =>
        Map[String, Any](
          "locationId" -> row.getAs[Int](FeatureColumns.LocationId),
          "originWindowStart" -> row.getAs[Long](FeatureColumns.WindowStart),
          "targetWindowStart" ->
            (row.getAs[Long](FeatureColumns.WindowStart) + (settings.horizonSlots.toLong * settings.slideMillis)),
          "forecast" -> row.getAs[Double](DemandForecaster.ForecastColumn),
          "lowerBound" -> row.getAs[Double](DemandForecaster.ForecastLowerColumn),
          "upperBound" -> row.getAs[Double](DemandForecaster.ForecastUpperColumn),
          "modelVersion" -> settings.modelVersion,
          "horizonSlots" -> settings.horizonSlots
        )
      }

    if (rows.isEmpty) {
      0L
    } else {
      val outcome = connector.writeBatch(PublishForecastCypher, rows.iterator, batchSize = 500)
      outcome.propertiesSet
    }
  }

  /** Loads the model, reusing the cached instance when the version is unchanged. */
  private def resolveModel(spark: SparkSession, forecaster: DemandForecaster): LoadedModel = {
    val savedVersion = readSavedModelVersion(spark)

    cachedModel.get() match {
      case Some(loaded) if savedVersion.contains(loaded.version) =>
        loaded

      case existing =>
        savedVersion.foreach { version =>
          existing.foreach(previous => logger.info(s"Model version changed from ${previous.version} to $version"))
        }
        val loaded = LoadedModel(
          version = savedVersion.getOrElse(settings.modelVersion),
          model = forecaster.load(settings.modelPath)
        )
        cachedModel.set(Some(loaded))
        loaded
    }
  }

  private def readSavedModelVersion(spark: SparkSession): Option[String] =
    try {
      spark.read
        .json(s"${settings.modelPath}/metadata")
        .select(col("modelVersion"))
        .collect()
        .headOption
        .map(_.getString(0))
    } catch {
      case NonFatal(error) =>
        logger.warn(s"Could not read the saved model version: ${error.getMessage}")
        None
    }

  // -------------------------------------------------------------------------
  // Shared helpers
  // -------------------------------------------------------------------------

  /** Newest window boundary the streaming stage has written. */
  def latestWindowStart(): Option[Long] =
    connector.readOne(
      """
        |MATCH (window:DemandWindow)
        |RETURN max(window.windowStart) AS latest
        |""".stripMargin
    )(record => record.get("latest")).flatMap { value =>
      if (value == null || value.isNull) None else Some(value.asLong())
    }

  /**
   * Returns the session the streaming stage created, or builds one for the
   * batch-only modes. A second session in the same JVM would share the same
   * SparkContext anyway, so reusing the active one avoids surprising
   * configuration divergence between the stages.
   */
  private def activeSession(): SparkSession =
    SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .getOrElse(
        SparkSession
          .builder()
          .appName(settings.appName)
          .config("spark.sql.shuffle.partitions", settings.shufflePartitions.toString)
          .config("spark.sql.session.timeZone", "UTC")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .getOrCreate()
      )

  private def newScheduler(): ScheduledExecutorService =
    Executors.newScheduledThreadPool(
      SchedulerThreads,
      new ThreadFactory {
        private val counter = new AtomicInteger(0)
        override def newThread(runnable: Runnable): Thread = {
          val thread = new Thread(runnable, s"pipeline-cycle-${counter.incrementAndGet()}")
          thread.setDaemon(true)
          thread
        }
      }
    )

  private def logStatus(): Unit =
    logger.info(
      s"driver status: graph_cycles=${graphCycles.get()} inference_cycles=${inferenceCycles.get()} " +
        s"graph_failures=${graphFailures.get()} inference_failures=${inferenceFailures.get()} " +
        s"model=${cachedModel.get().map(_.version).getOrElse("unloaded")}"
    )

  def requestShutdown(): Unit =
    if (running.compareAndSet(true, false)) {
      logger.warn("Shutdown requested")
      terminated.countDown()
    }

  private def registerShutdownHook(scheduler: ScheduledExecutorService): Unit =
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        requestShutdown()
        shutdownScheduler(scheduler)
      }
    }, "pipeline-driver-shutdown"))

  private def shutdownScheduler(scheduler: ScheduledExecutorService): Unit =
    try {
      scheduler.shutdown()
      if (!scheduler.awaitTermination(SchedulerShutdownSeconds, TimeUnit.SECONDS)) {
        val pending = scheduler.shutdownNow()
        logger.warn(s"Scheduler did not drain cleanly; ${pending.size()} tasks cancelled")
      }
    } catch {
      case _: InterruptedException =>
        val _ = scheduler.shutdownNow()
        Thread.currentThread().interrupt()
    }

  private def closeResources(): Unit = {
    connector.logMetrics()
    Neo4jConnector.closeAll()
  }
}

object PipelineDriver extends StrictLogging {

  private val SchedulerThreads = 3
  private val SchedulerShutdownSeconds = 60L
  private val StatusIntervalMinutes = 10L
  private val TopZonesLogged = 5

  /** Constraints the schema bootstrap is expected to have created. */
  private val RequiredConstraints: Seq[String] = Seq(
    "location_id_unique",
    "demand_window_key_unique"
  )

  /** A model together with the version label it was saved under. */
  private final case class LoadedModel(version: String, model: TrainedModel)

  /**
   * Upserts a forecast for a zone and target window. Zones are matched rather
   * than merged so a stale identifier cannot introduce a node the ingestion
   * path has never produced.
   */
  private val PublishForecastCypher: String =
    """
      |UNWIND $rows AS row
      |MATCH (zone:Location {locationId: row.locationId})
      |MERGE (forecast:DemandForecast {locationId: row.locationId, targetWindowStart: row.targetWindowStart})
      |SET forecast.originWindowStart = row.originWindowStart,
      |    forecast.forecastTripCount = row.forecast,
      |    forecast.lowerBound        = row.lowerBound,
      |    forecast.upperBound        = row.upperBound,
      |    forecast.modelVersion      = row.modelVersion,
      |    forecast.horizonSlots      = row.horizonSlots
      |MERGE (zone)-[:HAS_FORECAST]->(forecast)
      |""".stripMargin

  def main(args: Array[String]): Unit = {
    DriverSettings.parse(args) match {
      case Left(message) =>
        Console.err.println(message)
        if (message == DriverSettings.Usage) sys.exit(0) else sys.exit(1)

      case Right(settings) =>
        val exitCode =
          try new PipelineDriver(settings).run()
          catch {
            case NonFatal(error) =>
              logger.error(s"Driver failed to start: ${error.getMessage}", error)
              2
          }
        sys.exit(exitCode)
    }
  }
}
