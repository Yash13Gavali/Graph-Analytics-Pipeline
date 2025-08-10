package com.taxi.analytics.processor

import java.time.{Duration => JDuration}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamingQueryListener, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.neo4j.driver.{AuthTokens, Config, Driver, GraphDatabase, Logging, Session, SessionConfig, TransactionCallback, TransactionContext}

// ---------------------------------------------------------------------------
// Real-Time Graph Analytics & NYC Taxi Demand Forecasting
//
// Structured Streaming pipeline that consumes canonical trip events from the
// `nyc-taxi-trips` topic, deduplicates them within the watermark, and computes
// two sliding-window aggregations:
//
//   1. Per-zone demand  -> (:TaxiZone)-[:HAS_DEMAND_WINDOW]->(:DemandWindow)
//   2. Zone-to-zone flow -> (:TaxiZone)-[:TRIP_FLOW]->(:TaxiZone)
//
// Both are written to Neo4j through idempotent, batched Cypher so that a
// restart from the last checkpoint converges on the same graph state.
// ---------------------------------------------------------------------------

/** Connection parameters for the graph sink. Serialized to executors. */
final case class Neo4jSettings(
    uri: String,
    user: String,
    password: String,
    database: String,
    maxConnectionPoolSize: Int,
    connectionAcquisitionTimeoutSeconds: Int,
    maxTransactionRetrySeconds: Int,
    writeBatchSize: Int,
    maxWriteAttempts: Int
) extends Serializable

/** Full job configuration. */
final case class ProcessorSettings(
    appName: String,
    bootstrapServers: String,
    topic: String,
    consumerGroupPrefix: String,
    startingOffsets: String,
    maxOffsetsPerTrigger: Long,
    failOnDataLoss: Boolean,
    checkpointRoot: String,
    windowDuration: String,
    slideDuration: String,
    watermarkDelay: String,
    triggerIntervalSeconds: Int,
    shufflePartitions: Int,
    outputMode: OutputMode,
    neo4j: Neo4jSettings
)

object ProcessorSettings {

  private val DefaultTopic = "nyc-taxi-trips"

  val Usage: String =
    """
      |Usage: StreamProcessor [options]
      |
      |Kafka source:
      |  --bootstrap-servers <list>      Kafka bootstrap servers        (default: kafka:9092)
      |  --topic <name>                  Source topic                   (default: nyc-taxi-trips)
      |  --starting-offsets <spec>       earliest | latest | JSON spec  (default: latest)
      |  --max-offsets-per-trigger <n>   Rate cap per micro-batch, 0 = uncapped (default: 500000)
      |  --consumer-group-prefix <s>     Prefix for the source group id (default: taxi-stream-processor)
      |  --fail-on-data-loss <bool>      Abort on missing offsets       (default: true)
      |
      |Windowing:
      |  --window-duration <spec>        Sliding window width           (default: 15 minutes)
      |  --slide-duration <spec>         Slide interval                 (default: 5 minutes)
      |  --watermark-delay <spec>        Allowed event-time lateness    (default: 10 minutes)
      |  --trigger-interval <seconds>    Micro-batch cadence            (default: 30)
      |  --output-mode <mode>            update | append                (default: update)
      |
      |Execution:
      |  --app-name <name>               Spark application name         (default: nyc-taxi-stream-processor)
      |  --checkpoint-location <path>    Checkpoint root directory      (default: /opt/spark/checkpoints)
      |  --shuffle-partitions <n>        Shuffle parallelism            (default: 16)
      |
      |Neo4j sink:
      |  --neo4j-uri <uri>               Bolt URI                       (default: bolt://neo4j:7687)
      |  --neo4j-user <user>             Username                       (default: neo4j)
      |  --neo4j-password <password>     Password; prefer the NEO4J_PASSWORD environment variable
      |  --neo4j-database <name>         Target database                (default: neo4j)
      |  --neo4j-pool-size <n>           Max connection pool size       (default: 32)
      |  --write-batch-size <n>          Rows per Cypher transaction    (default: 2000)
      |  --max-write-attempts <n>        Retries per batch              (default: 5)
      |
      |  --help                          Print this message.
      |""".stripMargin

  def parse(args: Array[String]): Either[String, ProcessorSettings] = {
    val parsed = mutable.LinkedHashMap.empty[String, String]
    val flags = mutable.Set.empty[String]

    var index = 0
    while (index < args.length) {
      val token = args(index)
      if (!token.startsWith("--")) {
        return Left(s"Unexpected positional argument: $token")
      }
      val key = token.substring(2)
      if (key == "help") {
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

    def longOption(key: String, default: Long, minimum: Long): Either[String, Long] =
      optional(key) match {
        case None => Right(default)
        case Some(raw) =>
          try {
            val value = raw.toLong
            if (value < minimum) Left(s"--$key must be >= $minimum") else Right(value)
          } catch { case _: NumberFormatException => Left(s"--$key expects an integer, got '$raw'") }
      }

    def booleanOption(key: String, default: Boolean): Either[String, Boolean] =
      optional(key) match {
        case None => Right(default)
        case Some(raw) =>
          raw.toLowerCase match {
            case "true" | "yes" | "1" => Right(true)
            case "false" | "no" | "0" => Right(false)
            case other => Left(s"--$key expects a boolean, got '$other'")
          }
      }

    def durationOption(key: String, default: String): Either[String, String] = {
      val value = optional(key).getOrElse(default)
      if (IntervalSpec.isValid(value)) Right(value)
      else Left(s"--$key expects an interval such as '15 minutes', got '$value'")
    }

    for {
      password <- optional("neo4j-password")
        .orElse(Option(System.getenv("NEO4J_PASSWORD")))
        .toRight("Neo4j password must be supplied via --neo4j-password or the NEO4J_PASSWORD environment variable")
      windowDuration <- durationOption("window-duration", "15 minutes")
      slideDuration <- durationOption("slide-duration", "5 minutes")
      watermarkDelay <- durationOption("watermark-delay", "10 minutes")
      _ <- if (IntervalSpec.toSeconds(slideDuration) <= IntervalSpec.toSeconds(windowDuration)) Right(())
           else Left("--slide-duration must not exceed --window-duration")
      outputMode <- optional("output-mode").getOrElse("update").toLowerCase match {
        case "update" => Right(OutputMode.Update())
        case "append" => Right(OutputMode.Append())
        case other => Left(s"--output-mode must be 'update' or 'append', got '$other'")
      }
      maxOffsets <- longOption("max-offsets-per-trigger", 500000L, 0L)
      triggerInterval <- intOption("trigger-interval", 30, 1)
      shufflePartitions <- intOption("shuffle-partitions", 16, 1)
      poolSize <- intOption("neo4j-pool-size", 32, 1)
      writeBatchSize <- intOption("write-batch-size", 2000, 1)
      maxWriteAttempts <- intOption("max-write-attempts", 5, 1)
      failOnDataLoss <- booleanOption("fail-on-data-loss", default = true)
    } yield ProcessorSettings(
      appName = optional("app-name").getOrElse("nyc-taxi-stream-processor"),
      bootstrapServers = optional("bootstrap-servers").getOrElse("kafka:9092"),
      topic = optional("topic").getOrElse(DefaultTopic),
      consumerGroupPrefix = optional("consumer-group-prefix").getOrElse("taxi-stream-processor"),
      startingOffsets = optional("starting-offsets").getOrElse("latest"),
      maxOffsetsPerTrigger = maxOffsets,
      failOnDataLoss = failOnDataLoss,
      checkpointRoot = optional("checkpoint-location").getOrElse("/opt/spark/checkpoints"),
      windowDuration = windowDuration,
      slideDuration = slideDuration,
      watermarkDelay = watermarkDelay,
      triggerIntervalSeconds = triggerInterval,
      shufflePartitions = shufflePartitions,
      outputMode = outputMode,
      neo4j = Neo4jSettings(
        uri = optional("neo4j-uri").getOrElse("bolt://neo4j:7687"),
        user = optional("neo4j-user").getOrElse("neo4j"),
        password = password,
        database = optional("neo4j-database").getOrElse("neo4j"),
        maxConnectionPoolSize = poolSize,
        connectionAcquisitionTimeoutSeconds = 60,
        maxTransactionRetrySeconds = 30,
        writeBatchSize = writeBatchSize,
        maxWriteAttempts = maxWriteAttempts
      )
    )
  }
}

/** Validation and conversion for Spark interval strings. */
private[processor] object IntervalSpec {

  private val Pattern = """^\s*(\d+)\s+(second|seconds|minute|minutes|hour|hours)\s*$""".r

  def isValid(spec: String): Boolean = Pattern.findFirstIn(spec).isDefined

  def toSeconds(spec: String): Long = spec match {
    case Pattern(amount, unit) =>
      val magnitude = amount.toLong
      unit match {
        case "second" | "seconds" => magnitude
        case "minute" | "minutes" => magnitude * 60L
        case _ => magnitude * 3600L
      }
    case _ =>
      throw new IllegalArgumentException(s"Unsupported interval specification: $spec")
  }
}

/** Schema of the canonical trip event published by the ingestion module. */
private[processor] object TripEventSchema {

  val CorruptRecordColumn = "_malformed_payload"

  val schema: StructType = StructType(
    Seq(
      StructField("event_id", StringType, nullable = false),
      StructField("service_type", StringType, nullable = true),
      StructField("vendor_id", IntegerType, nullable = true),
      StructField("pickup_time", StringType, nullable = true),
      StructField("dropoff_time", StringType, nullable = true),
      StructField("pickup_epoch_millis", LongType, nullable = false),
      StructField("dropoff_epoch_millis", LongType, nullable = false),
      StructField("trip_duration_seconds", LongType, nullable = true),
      StructField("passenger_count", IntegerType, nullable = true),
      StructField("trip_distance_miles", DoubleType, nullable = true),
      StructField("average_speed_mph", DoubleType, nullable = true),
      StructField("pickup_location_id", IntegerType, nullable = false),
      StructField("dropoff_location_id", IntegerType, nullable = false),
      StructField("rate_code_id", IntegerType, nullable = true),
      StructField("payment_type", IntegerType, nullable = true),
      StructField("store_and_forward_flag", StringType, nullable = true),
      StructField("fare_amount", DoubleType, nullable = true),
      StructField("extra_amount", DoubleType, nullable = true),
      StructField("mta_tax", DoubleType, nullable = true),
      StructField("tip_amount", DoubleType, nullable = true),
      StructField("tolls_amount", DoubleType, nullable = true),
      StructField("improvement_surcharge", DoubleType, nullable = true),
      StructField("congestion_surcharge", DoubleType, nullable = true),
      StructField("airport_fee", DoubleType, nullable = true),
      StructField("ehail_fee", DoubleType, nullable = true),
      StructField("total_amount", DoubleType, nullable = true),
      StructField("source_file", StringType, nullable = true),
      StructField("source_line", LongType, nullable = true),
      StructField("ingested_at", StringType, nullable = true),
      StructField(CorruptRecordColumn, StringType, nullable = true)
    )
  )
}

/**
 * Per-JVM Neo4j driver holder. Executors obtain the driver lazily on first
 * write and reuse it for the lifetime of the process; the connection pool in
 * the driver handles concurrency across task threads.
 */
private[processor] object Neo4jConnection extends StrictLogging {

  @transient @volatile private var driverRef: Driver = _
  private val hookRegistered = new AtomicBoolean(false)

  def driver(settings: Neo4jSettings): Driver = {
    val existing = driverRef
    if (existing != null) {
      existing
    } else {
      synchronized {
        if (driverRef == null) {
          val config = Config.builder()
            .withMaxConnectionPoolSize(settings.maxConnectionPoolSize)
            .withConnectionAcquisitionTimeout(settings.connectionAcquisitionTimeoutSeconds.toLong, TimeUnit.SECONDS)
            .withMaxTransactionRetryTime(settings.maxTransactionRetrySeconds.toLong, TimeUnit.SECONDS)
            .withConnectionLivenessCheckTimeout(60L, TimeUnit.SECONDS)
            .withLogging(Logging.slf4j())
            .build()

          val created = GraphDatabase.driver(settings.uri, AuthTokens.basic(settings.user, settings.password), config)
          created.verifyConnectivity()
          driverRef = created
          registerShutdownHook()
          logger.info(s"Neo4j driver initialised for ${settings.uri} (database=${settings.database})")
        }
        driverRef
      }
    }
  }

  def withSession[T](settings: Neo4jSettings)(body: Session => T): T = {
    val session = driver(settings).session(SessionConfig.forDatabase(settings.database))
    try body(session)
    finally session.close()
  }

  private def registerShutdownHook(): Unit = {
    if (hookRegistered.compareAndSet(false, true)) {
      Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
        override def run(): Unit = close()
      }, "neo4j-driver-shutdown"))
    }
  }

  def close(): Unit = synchronized {
    if (driverRef != null) {
      try driverRef.close()
      catch { case NonFatal(error) => logger.warn(s"Neo4j driver close was not clean: ${error.getMessage}") }
      driverRef = null
    }
  }
}

/** Cypher statements backing the graph projections. */
private[processor] object GraphSchema {

  val Constraints: Seq[String] = Seq(
    "CREATE CONSTRAINT taxi_zone_id IF NOT EXISTS FOR (z:TaxiZone) REQUIRE z.zoneId IS UNIQUE",
    "CREATE CONSTRAINT demand_window_key IF NOT EXISTS FOR (w:DemandWindow) REQUIRE (w.zoneId, w.windowStart) IS UNIQUE"
  )

  val Indexes: Seq[String] = Seq(
    "CREATE INDEX demand_window_start IF NOT EXISTS FOR (w:DemandWindow) ON (w.windowStart)",
    "CREATE INDEX demand_window_trip_count IF NOT EXISTS FOR (w:DemandWindow) ON (w.tripCount)",
    "CREATE INDEX trip_flow_window_start IF NOT EXISTS FOR ()-[f:TRIP_FLOW]-() ON (f.windowStart)"
  )

  /**
   * Upserts a demand window and links it to its zone. The MERGE key is the
   * (zone, windowStart) pair, so re-processing a micro-batch after a restart
   * overwrites rather than duplicates.
   */
  val UpsertDemandWindows: String =
    """
      |UNWIND $rows AS row
      |MERGE (zone:TaxiZone {zoneId: row.zoneId})
      |MERGE (bucket:DemandWindow {zoneId: row.zoneId, windowStart: row.windowStart})
      |SET bucket.windowEnd            = row.windowEnd,
      |    bucket.windowSeconds        = row.windowSeconds,
      |    bucket.tripCount            = row.tripCount,
      |    bucket.passengerTotal       = row.passengerTotal,
      |    bucket.demandPerMinute      = row.demandPerMinute,
      |    bucket.avgTripDistanceMiles = row.avgTripDistanceMiles,
      |    bucket.avgDurationSeconds   = row.avgDurationSeconds,
      |    bucket.avgSpeedMph          = row.avgSpeedMph,
      |    bucket.avgFareAmount        = row.avgFareAmount,
      |    bucket.totalRevenue         = row.totalRevenue,
      |    bucket.totalTips            = row.totalTips,
      |    bucket.distinctDestinations = row.distinctDestinations
      |MERGE (zone)-[:HAS_DEMAND_WINDOW]->(bucket)
      |""".stripMargin

  /**
   * Upserts the directed zone-to-zone flow edge for a window. The window start
   * participates in the relationship MERGE key so that consecutive windows
   * accumulate as distinct edges rather than clobbering one another.
   */
  val UpsertZoneFlows: String =
    """
      |UNWIND $rows AS row
      |MERGE (origin:TaxiZone {zoneId: row.pickupZoneId})
      |MERGE (destination:TaxiZone {zoneId: row.dropoffZoneId})
      |MERGE (origin)-[flow:TRIP_FLOW {windowStart: row.windowStart}]->(destination)
      |SET flow.windowEnd            = row.windowEnd,
      |    flow.tripCount            = row.tripCount,
      |    flow.avgDurationSeconds   = row.avgDurationSeconds,
      |    flow.avgTripDistanceMiles = row.avgTripDistanceMiles,
      |    flow.avgSpeedMph          = row.avgSpeedMph,
      |    flow.totalRevenue         = row.totalRevenue
      |""".stripMargin
}

/**
 * Executes batched, retrying Cypher writes for a partition of rows. Instances
 * are constructed on the executor, so only the serializable settings and the
 * statement text cross the wire.
 */
private[processor] object CypherBatchWriter extends StrictLogging {

  def writePartition(
      rows: Iterator[Row],
      settings: Neo4jSettings,
      statement: String,
      toParameters: Row => JMap[String, Object]
  ): Unit = {
    if (rows.hasNext) {
      Neo4jConnection.withSession(settings) { session =>
        rows.grouped(settings.writeBatchSize).foreach { chunk =>
          val payload: JList[JMap[String, Object]] = new JArrayList[JMap[String, Object]](chunk.size)
          chunk.foreach(row => payload.add(toParameters(row)))
          executeWithRetry(session, statement, payload, settings)
        }
      }
    }
  }

  private def executeWithRetry(
      session: Session,
      statement: String,
      payload: JList[JMap[String, Object]],
      settings: Neo4jSettings
  ): Unit = {
    val parameters: JMap[String, Object] = new JHashMap[String, Object](1)
    val _ = parameters.put("rows", payload.asInstanceOf[Object])

    var attempt = 1
    var completed = false
    var lastError: Throwable = null

    while (!completed && attempt <= settings.maxWriteAttempts) {
      try {
        val _ = session.executeWrite(new TransactionCallback[java.lang.Integer] {
          override def execute(tx: TransactionContext): java.lang.Integer = {
            val result = tx.run(statement, parameters)
            val _ = result.consume()
            java.lang.Integer.valueOf(payload.size())
          }
        })
        completed = true
      } catch {
        case NonFatal(error) =>
          lastError = error
          if (attempt >= settings.maxWriteAttempts) {
            logger.error(s"Cypher batch failed after $attempt attempts: ${error.getMessage}", error)
          } else {
            val backoffMillis = math.min(15000L, 250L * math.pow(2.0, attempt.toDouble).toLong)
            logger.warn(s"Cypher batch attempt $attempt failed (${error.getMessage}); retrying in ${backoffMillis}ms")
            Thread.sleep(backoffMillis)
          }
          attempt += 1
      }
    }

    if (!completed) {
      throw new IllegalStateException(
        s"Failed to write ${payload.size()} rows to Neo4j after ${settings.maxWriteAttempts} attempts",
        lastError
      )
    }
  }

  /** Null-safe accessors that keep Cypher parameters strictly typed. */
  def longOf(row: Row, field: String): java.lang.Long = {
    val index = row.fieldIndex(field)
    if (row.isNullAt(index)) java.lang.Long.valueOf(0L) else java.lang.Long.valueOf(row.getLong(index))
  }

  def intOf(row: Row, field: String): java.lang.Integer = {
    val index = row.fieldIndex(field)
    if (row.isNullAt(index)) java.lang.Integer.valueOf(0) else java.lang.Integer.valueOf(row.getInt(index))
  }

  def doubleOf(row: Row, field: String): java.lang.Double = {
    val index = row.fieldIndex(field)
    if (row.isNullAt(index)) java.lang.Double.valueOf(0.0) else java.lang.Double.valueOf(row.getDouble(index))
  }
}

/** Logs micro-batch progress and the observed ingestion quality metrics. */
private[processor] final class ProgressListener extends StreamingQueryListener with StrictLogging {

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit =
    logger.info(s"Query started: name=${event.name} id=${event.id}")

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val progress = event.progress
    val inputRate = Option(progress.inputRowsPerSecond).map(_.doubleValue()).getOrElse(0.0)
    val processRate = Option(progress.processedRowsPerSecond).map(_.doubleValue()).getOrElse(0.0)

    logger.info(
      f"batch name=${progress.name}%s id=${progress.batchId}%d rows=${progress.numInputRows}%d " +
        f"input_rate=$inputRate%.1f/s process_rate=$processRate%.1f/s"
    )

    val observed = progress.observedMetrics.asScala.get(StreamProcessor.QualityMetricsName)
    observed.foreach { metrics =>
      val total = metrics.getAs[Long]("payloads_seen")
      val malformed = metrics.getAs[Long]("payloads_malformed")
      if (malformed > 0L) {
        logger.warn(s"payload quality: seen=$total malformed=$malformed")
      }
    }

    if (progress.sources.nonEmpty) {
      progress.sources.foreach { source =>
        logger.debug(s"source=${source.description} start=${source.startOffset} end=${source.endOffset}")
      }
    }
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit =
    event.exception match {
      case Some(reason) => logger.error(s"Query terminated with error: id=${event.id} reason=$reason")
      case None => logger.info(s"Query terminated cleanly: id=${event.id}")
    }
}

/** Builds and runs the streaming topology. */
final class StreamProcessor(settings: ProcessorSettings) extends StrictLogging {

  import StreamProcessor._

  def run(): Unit = {
    val spark = buildSparkSession()
    spark.streams.addListener(new ProgressListener)

    try {
      applyGraphSchema()

      val trips = parsedTripStream(spark)
      val demandQuery = startDemandQuery(trips)
      val flowQuery = startFlowQuery(trips)

      registerShutdownHook(Seq(demandQuery, flowQuery))

      logger.info(
        s"Streaming topology active: window=${settings.windowDuration} slide=${settings.slideDuration} " +
          s"watermark=${settings.watermarkDelay} trigger=${settings.triggerIntervalSeconds}s"
      )

      spark.streams.awaitAnyTermination()
    } finally {
      Neo4jConnection.close()
      spark.stop()
    }
  }

  private def buildSparkSession(): SparkSession =
    SparkSession
      .builder()
      .appName(settings.appName)
      .config("spark.sql.shuffle.partitions", settings.shufflePartitions.toString)
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
      .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
      .config("spark.sql.streaming.metricsEnabled", "true")
      .config("spark.sql.streaming.multipleWatermarkPolicy", "min")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrationRequired", "false")
      .getOrCreate()

  /** Applies constraints and indexes once, from the driver, before streaming. */
  private def applyGraphSchema(): Unit = {
    val statements = GraphSchema.Constraints ++ GraphSchema.Indexes
    Neo4jConnection.withSession(settings.neo4j) { session =>
      statements.foreach { statement =>
        val _ = session.executeWrite(new TransactionCallback[java.lang.Boolean] {
          override def execute(tx: TransactionContext): java.lang.Boolean = {
            val _ = tx.run(statement).consume()
            java.lang.Boolean.TRUE
          }
        })
      }
    }
    logger.info(s"Graph schema applied: ${statements.size} constraint and index statements")
  }

  /**
   * Reads the topic, parses the JSON envelope, drops malformed and duplicate
   * events, and projects the event-time column used by both aggregations.
   */
  private def parsedTripStream(spark: SparkSession): DataFrame = {
    val reader = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", settings.bootstrapServers)
      .option("subscribe", settings.topic)
      .option("startingOffsets", settings.startingOffsets)
      .option("failOnDataLoss", settings.failOnDataLoss.toString)
      .option("kafka.group.id", s"${settings.consumerGroupPrefix}-${settings.topic}")
      .option("kafka.client.dns.lookup", "use_all_dns_ips")
      .option("includeHeaders", "false")

    val rateLimited =
      if (settings.maxOffsetsPerTrigger > 0L) reader.option("maxOffsetsPerTrigger", settings.maxOffsetsPerTrigger.toString)
      else reader

    val raw = rateLimited.load()

    val decoded = raw
      .select(
        col("value").cast(StringType).as("payload"),
        col("partition").as("kafka_partition"),
        col("offset").as("kafka_offset")
      )
      .withColumn(
        "event",
        from_json(
          col("payload"),
          TripEventSchema.schema,
          Map(
            "mode" -> "PERMISSIVE",
            "columnNameOfCorruptRecord" -> TripEventSchema.CorruptRecordColumn
          )
        )
      )

    val observed = decoded
      .withColumn(
        "is_malformed",
        col("event").isNull ||
          col(s"event.${TripEventSchema.CorruptRecordColumn}").isNotNull ||
          col("event.event_id").isNull ||
          col("event.pickup_epoch_millis").isNull
      )
      .observe(
        QualityMetricsName,
        count(lit(1)).as("payloads_seen"),
        sum(when(col("is_malformed"), lit(1L)).otherwise(lit(0L))).as("payloads_malformed")
      )

    observed
      .filter(!col("is_malformed"))
      .select(col("event.*"), col("kafka_partition"), col("kafka_offset"))
      .drop(TripEventSchema.CorruptRecordColumn)
      .withColumn("pickup_time", timestamp_millis(col("pickup_epoch_millis")))
      .withColumn("dropoff_time", timestamp_millis(col("dropoff_epoch_millis")))
      .withWatermark("pickup_time", settings.watermarkDelay)
      .dropDuplicatesWithinWatermark("event_id")
  }

  /** Sliding-window demand per pickup zone. */
  private def startDemandQuery(trips: DataFrame): StreamingQuery = {
    val windowSeconds = IntervalSpec.toSeconds(settings.windowDuration)
    val windowMinutes = math.max(1.0, windowSeconds.toDouble / 60.0)

    val demand = trips
      .groupBy(
        window(col("pickup_time"), settings.windowDuration, settings.slideDuration).as("time_window"),
        col("pickup_location_id").as("zone_id")
      )
      .agg(
        count(lit(1)).as("trip_count"),
        sum(coalesce(col("passenger_count"), lit(0))).as("passenger_total"),
        avg(col("trip_distance_miles")).as("avg_trip_distance_miles"),
        avg(col("trip_duration_seconds").cast(DoubleType)).as("avg_duration_seconds"),
        avg(col("average_speed_mph")).as("avg_speed_mph"),
        avg(col("fare_amount")).as("avg_fare_amount"),
        sum(coalesce(col("total_amount"), lit(0.0))).as("total_revenue"),
        sum(coalesce(col("tip_amount"), lit(0.0))).as("total_tips"),
        approx_count_distinct(col("dropoff_location_id")).as("distinct_destinations")
      )
      .select(
        col("zone_id").cast(IntegerType).as("zone_id"),
        (col("time_window.start").cast(LongType) * lit(1000L)).as("window_start"),
        (col("time_window.end").cast(LongType) * lit(1000L)).as("window_end"),
        lit(windowSeconds).as("window_seconds"),
        col("trip_count").cast(LongType).as("trip_count"),
        col("passenger_total").cast(LongType).as("passenger_total"),
        (col("trip_count").cast(DoubleType) / lit(windowMinutes)).as("demand_per_minute"),
        coalesce(round(col("avg_trip_distance_miles"), 4), lit(0.0)).as("avg_trip_distance_miles"),
        coalesce(round(col("avg_duration_seconds"), 2), lit(0.0)).as("avg_duration_seconds"),
        coalesce(round(col("avg_speed_mph"), 3), lit(0.0)).as("avg_speed_mph"),
        coalesce(round(col("avg_fare_amount"), 4), lit(0.0)).as("avg_fare_amount"),
        round(col("total_revenue"), 2).as("total_revenue"),
        round(col("total_tips"), 2).as("total_tips"),
        col("distinct_destinations").cast(LongType).as("distinct_destinations")
      )

    val sinkSettings = settings.neo4j

    demand.writeStream
      .queryName("zone-demand-windows")
      .outputMode(settings.outputMode)
      .option("checkpointLocation", s"${settings.checkpointRoot}/zone-demand-windows")
      .trigger(Trigger.ProcessingTime(settings.triggerIntervalSeconds.toLong, TimeUnit.SECONDS))
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        val persisted = batch.persist()
        try {
          val rowCount = persisted.count()
          if (rowCount > 0L) {
            persisted.foreachPartition { rows: Iterator[Row] =>
              CypherBatchWriter.writePartition(
                rows,
                sinkSettings,
                GraphSchema.UpsertDemandWindows,
                demandParameters
              )
            }
          }
          logger.info(s"zone-demand-windows batch=$batchId windows_upserted=$rowCount")
        } finally {
          val _ = persisted.unpersist(blocking = false)
        }
      }
      .start()
  }

  /** Sliding-window directed flow between pickup and dropoff zones. */
  private def startFlowQuery(trips: DataFrame): StreamingQuery = {
    val flows = trips
      .groupBy(
        window(col("pickup_time"), settings.windowDuration, settings.slideDuration).as("time_window"),
        col("pickup_location_id").as("pickup_zone_id"),
        col("dropoff_location_id").as("dropoff_zone_id")
      )
      .agg(
        count(lit(1)).as("trip_count"),
        avg(col("trip_duration_seconds").cast(DoubleType)).as("avg_duration_seconds"),
        avg(col("trip_distance_miles")).as("avg_trip_distance_miles"),
        avg(col("average_speed_mph")).as("avg_speed_mph"),
        sum(coalesce(col("total_amount"), lit(0.0))).as("total_revenue")
      )
      .select(
        col("pickup_zone_id").cast(IntegerType).as("pickup_zone_id"),
        col("dropoff_zone_id").cast(IntegerType).as("dropoff_zone_id"),
        (col("time_window.start").cast(LongType) * lit(1000L)).as("window_start"),
        (col("time_window.end").cast(LongType) * lit(1000L)).as("window_end"),
        col("trip_count").cast(LongType).as("trip_count"),
        coalesce(round(col("avg_duration_seconds"), 2), lit(0.0)).as("avg_duration_seconds"),
        coalesce(round(col("avg_trip_distance_miles"), 4), lit(0.0)).as("avg_trip_distance_miles"),
        coalesce(round(col("avg_speed_mph"), 3), lit(0.0)).as("avg_speed_mph"),
        round(col("total_revenue"), 2).as("total_revenue")
      )

    val sinkSettings = settings.neo4j

    flows.writeStream
      .queryName("zone-trip-flows")
      .outputMode(settings.outputMode)
      .option("checkpointLocation", s"${settings.checkpointRoot}/zone-trip-flows")
      .trigger(Trigger.ProcessingTime(settings.triggerIntervalSeconds.toLong, TimeUnit.SECONDS))
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        val persisted = batch.persist()
        try {
          val rowCount = persisted.count()
          if (rowCount > 0L) {
            persisted.foreachPartition { rows: Iterator[Row] =>
              CypherBatchWriter.writePartition(
                rows,
                sinkSettings,
                GraphSchema.UpsertZoneFlows,
                flowParameters
              )
            }
          }
          logger.info(s"zone-trip-flows batch=$batchId edges_upserted=$rowCount")
        } finally {
          val _ = persisted.unpersist(blocking = false)
        }
      }
      .start()
  }

  private def registerShutdownHook(queries: Seq[StreamingQuery]): Unit =
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        logger.warn("Shutdown signal received; stopping streaming queries")
        queries.foreach { query =>
          try if (query.isActive) query.stop()
          catch { case NonFatal(error) => logger.warn(s"Query stop was not clean: ${error.getMessage}") }
        }
        Neo4jConnection.close()
      }
    }, "stream-processor-shutdown"))
}

object StreamProcessor extends StrictLogging {

  val QualityMetricsName = "trip_payload_quality"

  /** Row-to-Cypher parameter mapping for demand windows. Executed on executors. */
  private[processor] val demandParameters: Row => JMap[String, Object] = { row =>
    val parameters: JMap[String, Object] = new JHashMap[String, Object](14)
    val _ = parameters.put("zoneId", CypherBatchWriter.intOf(row, "zone_id"))
    val _ = parameters.put("windowStart", CypherBatchWriter.longOf(row, "window_start"))
    val _ = parameters.put("windowEnd", CypherBatchWriter.longOf(row, "window_end"))
    val _ = parameters.put("windowSeconds", CypherBatchWriter.longOf(row, "window_seconds"))
    val _ = parameters.put("tripCount", CypherBatchWriter.longOf(row, "trip_count"))
    val _ = parameters.put("passengerTotal", CypherBatchWriter.longOf(row, "passenger_total"))
    val _ = parameters.put("demandPerMinute", CypherBatchWriter.doubleOf(row, "demand_per_minute"))
    val _ = parameters.put("avgTripDistanceMiles", CypherBatchWriter.doubleOf(row, "avg_trip_distance_miles"))
    val _ = parameters.put("avgDurationSeconds", CypherBatchWriter.doubleOf(row, "avg_duration_seconds"))
    val _ = parameters.put("avgSpeedMph", CypherBatchWriter.doubleOf(row, "avg_speed_mph"))
    val _ = parameters.put("avgFareAmount", CypherBatchWriter.doubleOf(row, "avg_fare_amount"))
    val _ = parameters.put("totalRevenue", CypherBatchWriter.doubleOf(row, "total_revenue"))
    val _ = parameters.put("totalTips", CypherBatchWriter.doubleOf(row, "total_tips"))
    val _ = parameters.put("distinctDestinations", CypherBatchWriter.longOf(row, "distinct_destinations"))
    parameters
  }

  /** Row-to-Cypher parameter mapping for zone flow edges. Executed on executors. */
  private[processor] val flowParameters: Row => JMap[String, Object] = { row =>
    val parameters: JMap[String, Object] = new JHashMap[String, Object](9)
    val _ = parameters.put("pickupZoneId", CypherBatchWriter.intOf(row, "pickup_zone_id"))
    val _ = parameters.put("dropoffZoneId", CypherBatchWriter.intOf(row, "dropoff_zone_id"))
    val _ = parameters.put("windowStart", CypherBatchWriter.longOf(row, "window_start"))
    val _ = parameters.put("windowEnd", CypherBatchWriter.longOf(row, "window_end"))
    val _ = parameters.put("tripCount", CypherBatchWriter.longOf(row, "trip_count"))
    val _ = parameters.put("avgDurationSeconds", CypherBatchWriter.doubleOf(row, "avg_duration_seconds"))
    val _ = parameters.put("avgTripDistanceMiles", CypherBatchWriter.doubleOf(row, "avg_trip_distance_miles"))
    val _ = parameters.put("avgSpeedMph", CypherBatchWriter.doubleOf(row, "avg_speed_mph"))
    val _ = parameters.put("totalRevenue", CypherBatchWriter.doubleOf(row, "total_revenue"))
    parameters
  }

  def main(args: Array[String]): Unit = {
    ProcessorSettings.parse(args) match {
      case Left(message) =>
        Console.err.println(message)
        if (message == ProcessorSettings.Usage) sys.exit(0) else sys.exit(1)

      case Right(settings) =>
        try {
          new StreamProcessor(settings).run()
          sys.exit(0)
        } catch {
          case NonFatal(error) =>
            logger.error(s"Stream processor failed: ${error.getMessage}", error)
            sys.exit(2)
        }
    }
  }
}
