package com.taxi.analytics.ingestion

import java.io.{BufferedReader, FileInputStream, InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.{Duration => JDuration, Instant, LocalDateTime, ZoneId}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{CountDownLatch, Executors, ScheduledExecutorService, TimeUnit}
import java.util.zip.GZIPInputStream
import java.util.{Properties, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import com.typesafe.scalalogging.StrictLogging
import io.circe.Json
import io.circe.syntax._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig => KafkaProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.StringSerializer

// ---------------------------------------------------------------------------
// Real-Time Graph Analytics & NYC Taxi Demand Forecasting
//
// Replays NYC TLC trip-record CSV extracts into the `nyc-taxi-trips` Kafka
// topic as canonical JSON events. The producer is schema-tolerant across the
// yellow and green TLC layouts, partitions by pickup zone so that per-zone
// event ordering is preserved for the downstream graph builder, and emits
// deterministic event identifiers so that a replay is idempotent at the
// consumer side.
// ---------------------------------------------------------------------------

/** Immutable runtime configuration for the producer. */
final case class ProducerSettings(
    input: Path,
    bootstrapServers: String,
    topic: String,
    serviceType: String,
    clientId: String,
    recordsPerSecond: Int,
    maxRecords: Long,
    reportIntervalSeconds: Int,
    compressionType: String,
    lingerMs: Int,
    batchSizeBytes: Int,
    bufferMemoryBytes: Long,
    requestTimeoutMs: Int,
    deliveryTimeoutMs: Int,
    sourceTimeZone: ZoneId,
    failFast: Boolean
)

object ProducerSettings {

  private val DefaultTopic = "nyc-taxi-trips"
  private val DefaultBootstrap = "localhost:29092"
  private val DefaultServiceType = "yellow"
  private val DefaultZone = "America/New_York"

  private val ValidServiceTypes = Set("yellow", "green", "fhv", "hvfhv")
  private val ValidCompression = Set("none", "gzip", "snappy", "lz4", "zstd")

  val Usage: String =
    """
      |Usage: TaxiTripProducer --input <path> [options]
      |
      |Required:
      |  --input <path>                CSV file, .csv.gz file, or directory of either.
      |
      |Options:
      |  --bootstrap-servers <list>    Kafka bootstrap servers        (default: localhost:29092)
      |  --topic <name>                Destination topic              (default: nyc-taxi-trips)
      |  --service-type <type>         yellow | green | fhv | hvfhv   (default: yellow)
      |  --client-id <id>              Kafka client identifier        (default: derived from host)
      |  --rate <n>                    Records per second, 0 = unthrottled (default: 0)
      |  --limit <n>                   Stop after n accepted records, 0 = no limit (default: 0)
      |  --report-interval <seconds>   Throughput log cadence         (default: 10)
      |  --compression <codec>         none|gzip|snappy|lz4|zstd      (default: snappy)
      |  --linger-ms <n>               Producer linger                (default: 20)
      |  --batch-size <bytes>          Producer batch size            (default: 262144)
      |  --buffer-memory <bytes>       Producer buffer memory         (default: 67108864)
      |  --request-timeout-ms <n>      Broker request timeout         (default: 30000)
      |  --delivery-timeout-ms <n>     Total delivery timeout         (default: 120000)
      |  --source-time-zone <zone>     Zone of the CSV local times    (default: America/New_York)
      |  --fail-fast                   Abort on the first rejected row rather than counting it.
      |  --help                        Print this message.
      |""".stripMargin

  /** Parses `--key value` pairs and boolean flags into settings. */
  def parse(args: Array[String]): Either[String, ProducerSettings] = {
    val parsed = mutable.LinkedHashMap.empty[String, String]
    val flags = mutable.Set.empty[String]

    var index = 0
    while (index < args.length) {
      val token = args(index)
      if (!token.startsWith("--")) {
        return Left(s"Unexpected positional argument: $token")
      }
      val key = token.substring(2)
      if (key == "help" || key == "fail-fast") {
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

    def optional(key: String): Option[String] =
      parsed.get(key).map(_.trim).filter(_.nonEmpty)

    def intOption(key: String, default: Int, minimum: Int): Either[String, Int] =
      optional(key) match {
        case None => Right(default)
        case Some(raw) =>
          try {
            val value = raw.toInt
            if (value < minimum) Left(s"--$key must be >= $minimum") else Right(value)
          } catch {
            case _: NumberFormatException => Left(s"--$key expects an integer, got '$raw'")
          }
      }

    def longOption(key: String, default: Long, minimum: Long): Either[String, Long] =
      optional(key) match {
        case None => Right(default)
        case Some(raw) =>
          try {
            val value = raw.toLong
            if (value < minimum) Left(s"--$key must be >= $minimum") else Right(value)
          } catch {
            case _: NumberFormatException => Left(s"--$key expects an integer, got '$raw'")
          }
      }

    for {
      rawInput <- optional("input").toRight("--input is required")
      inputPath <- {
        val candidate = Paths.get(rawInput).toAbsolutePath.normalize()
        if (!Files.exists(candidate)) Left(s"Input path does not exist: $candidate")
        else if (!Files.isReadable(candidate)) Left(s"Input path is not readable: $candidate")
        else Right(candidate)
      }
      serviceType <- {
        val value = optional("service-type").getOrElse(DefaultServiceType).toLowerCase
        if (ValidServiceTypes.contains(value)) Right(value)
        else Left(s"--service-type must be one of ${ValidServiceTypes.toSeq.sorted.mkString(", ")}")
      }
      compression <- {
        val value = optional("compression").getOrElse("snappy").toLowerCase
        if (ValidCompression.contains(value)) Right(value)
        else Left(s"--compression must be one of ${ValidCompression.toSeq.sorted.mkString(", ")}")
      }
      zone <- {
        val value = optional("source-time-zone").getOrElse(DefaultZone)
        try Right(ZoneId.of(value))
        catch { case NonFatal(_) => Left(s"--source-time-zone is not a valid zone id: $value") }
      }
      rate <- intOption("rate", 0, 0)
      limit <- longOption("limit", 0L, 0L)
      reportInterval <- intOption("report-interval", 10, 1)
      lingerMs <- intOption("linger-ms", 20, 0)
      batchSize <- intOption("batch-size", 262144, 1024)
      bufferMemory <- longOption("buffer-memory", 67108864L, 1048576L)
      requestTimeout <- intOption("request-timeout-ms", 30000, 1000)
      deliveryTimeout <- intOption("delivery-timeout-ms", 120000, 1000)
      _ <- if (deliveryTimeout >= requestTimeout + lingerMs) Right(())
           else Left("--delivery-timeout-ms must be >= --request-timeout-ms + --linger-ms")
    } yield ProducerSettings(
      input = inputPath,
      bootstrapServers = optional("bootstrap-servers").getOrElse(DefaultBootstrap),
      topic = optional("topic").getOrElse(DefaultTopic),
      serviceType = serviceType,
      clientId = optional("client-id").getOrElse(defaultClientId(serviceType)),
      recordsPerSecond = rate,
      maxRecords = limit,
      reportIntervalSeconds = reportInterval,
      compressionType = compression,
      lingerMs = lingerMs,
      batchSizeBytes = batchSize,
      bufferMemoryBytes = bufferMemory,
      requestTimeoutMs = requestTimeout,
      deliveryTimeoutMs = deliveryTimeout,
      sourceTimeZone = zone,
      failFast = flags.contains("fail-fast")
    )
  }

  private def defaultClientId(serviceType: String): String = {
    val host =
      try java.net.InetAddress.getLocalHost.getHostName
      catch { case NonFatal(_) => "unknown-host" }
    s"taxi-trip-producer-$serviceType-$host-${ProcessIdentity.current}"
  }
}

/** Best-effort JVM process identity, used only to disambiguate client ids. */
private[ingestion] object ProcessIdentity {
  val current: String = {
    val name = java.lang.management.ManagementFactory.getRuntimeMXBean.getName
    val at = name.indexOf('@')
    if (at > 0) name.substring(0, at) else UUID.randomUUID().toString.take(8)
  }
}

/** RFC 4180 compliant single-line CSV splitter. */
private[ingestion] object CsvLineParser {

  def parse(line: String, delimiter: Char): Vector[String] = {
    val fields = Vector.newBuilder[String]
    val current = new java.lang.StringBuilder(64)
    var inQuotes = false
    var i = 0

    while (i < line.length) {
      val c = line.charAt(i)
      if (inQuotes) {
        if (c == '"') {
          if (i + 1 < line.length && line.charAt(i + 1) == '"') {
            current.append('"')
            i += 1
          } else {
            inQuotes = false
          }
        } else {
          current.append(c)
        }
      } else if (c == '"') {
        inQuotes = true
      } else if (c == delimiter) {
        fields += current.toString
        current.setLength(0)
      } else {
        current.append(c)
      }
      i += 1
    }

    fields += current.toString
    fields.result()
  }
}

/**
 * Resolves logical column names against the physical header of a TLC extract.
 * The yellow and green layouts differ only in their datetime column prefixes
 * and in a handful of optional surcharge columns, so a single alias table
 * covers both without branching at the call site.
 */
private[ingestion] final class ColumnIndex private (private val positions: Map[String, Int]) {

  def optionalIndex(logicalName: String): Option[Int] = positions.get(logicalName)

  def has(logicalName: String): Boolean = positions.contains(logicalName)

  def missing(required: Seq[String]): Seq[String] = required.filterNot(positions.contains)
}

private[ingestion] object ColumnIndex {

  private val Aliases: Map[String, Seq[String]] = Map(
    "vendor_id" -> Seq("vendorid", "vendorid1", "vendor_id", "vendorname"),
    "pickup_datetime" -> Seq("tpeppickupdatetime", "lpeppickupdatetime", "pickupdatetime", "pickup_datetime", "pickupdate"),
    "dropoff_datetime" -> Seq("tpepdropoffdatetime", "lpepdropoffdatetime", "dropoffdatetime", "dropoff_datetime", "dropoffdate"),
    "passenger_count" -> Seq("passengercount"),
    "trip_distance" -> Seq("tripdistance"),
    "ratecode_id" -> Seq("ratecodeid", "ratecode"),
    "store_and_fwd_flag" -> Seq("storeandfwdflag"),
    "pickup_location_id" -> Seq("pulocationid", "pickuplocationid"),
    "dropoff_location_id" -> Seq("dolocationid", "dropofflocationid"),
    "payment_type" -> Seq("paymenttype"),
    "fare_amount" -> Seq("fareamount"),
    "extra" -> Seq("extra"),
    "mta_tax" -> Seq("mtatax"),
    "tip_amount" -> Seq("tipamount"),
    "tolls_amount" -> Seq("tollsamount"),
    "improvement_surcharge" -> Seq("improvementsurcharge"),
    "total_amount" -> Seq("totalamount"),
    "congestion_surcharge" -> Seq("congestionsurcharge"),
    "airport_fee" -> Seq("airportfee", "airport_fee"),
    "trip_type" -> Seq("triptype"),
    "ehail_fee" -> Seq("ehailfee")
  )

  val RequiredColumns: Seq[String] = Seq(
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "trip_distance",
    "total_amount"
  )

  private def normalize(raw: String): String =
    raw.trim.toLowerCase.filter(ch => ch.isLetterOrDigit)

  def fromHeader(headerFields: Vector[String]): ColumnIndex = {
    val normalizedHeader: Map[String, Int] =
      headerFields.zipWithIndex.map { case (name, idx) => normalize(name) -> idx }.toMap

    val resolved = Aliases.flatMap { case (logical, aliases) =>
      aliases.collectFirst { case alias if normalizedHeader.contains(alias) => logical -> normalizedHeader(alias) }
    }

    new ColumnIndex(resolved)
  }
}

/** Canonical trip event emitted to Kafka. */
final case class TripEvent(
    eventId: String,
    serviceType: String,
    vendorId: Option[Int],
    pickupTime: Instant,
    dropoffTime: Instant,
    tripDurationSeconds: Long,
    passengerCount: Option[Int],
    tripDistanceMiles: Double,
    averageSpeedMph: Option[Double],
    pickupLocationId: Int,
    dropoffLocationId: Int,
    rateCodeId: Option[Int],
    paymentType: Option[Int],
    storeAndForwardFlag: Option[String],
    fareAmount: Option[Double],
    extraAmount: Option[Double],
    mtaTax: Option[Double],
    tipAmount: Option[Double],
    tollsAmount: Option[Double],
    improvementSurcharge: Option[Double],
    congestionSurcharge: Option[Double],
    airportFee: Option[Double],
    ehailFee: Option[Double],
    totalAmount: Double,
    sourceFile: String,
    sourceLine: Long,
    ingestedAt: Instant
) {

  /** Partition key: pickup zone, so that per-zone ordering survives the topic. */
  def partitionKey: String = pickupLocationId.toString

  def asJson: Json = {
    def num(value: Option[Double]): Json = value.fold(Json.Null)(Json.fromDoubleOrNull)
    def int(value: Option[Int]): Json = value.fold(Json.Null)(Json.fromInt)
    def str(value: Option[String]): Json = value.fold(Json.Null)(Json.fromString)

    Json.obj(
      "event_id" -> Json.fromString(eventId),
      "service_type" -> Json.fromString(serviceType),
      "vendor_id" -> int(vendorId),
      "pickup_time" -> Json.fromString(pickupTime.toString),
      "dropoff_time" -> Json.fromString(dropoffTime.toString),
      "pickup_epoch_millis" -> Json.fromLong(pickupTime.toEpochMilli),
      "dropoff_epoch_millis" -> Json.fromLong(dropoffTime.toEpochMilli),
      "trip_duration_seconds" -> Json.fromLong(tripDurationSeconds),
      "passenger_count" -> int(passengerCount),
      "trip_distance_miles" -> Json.fromDoubleOrNull(tripDistanceMiles),
      "average_speed_mph" -> num(averageSpeedMph),
      "pickup_location_id" -> Json.fromInt(pickupLocationId),
      "dropoff_location_id" -> Json.fromInt(dropoffLocationId),
      "rate_code_id" -> int(rateCodeId),
      "payment_type" -> int(paymentType),
      "store_and_forward_flag" -> str(storeAndForwardFlag),
      "fare_amount" -> num(fareAmount),
      "extra_amount" -> num(extraAmount),
      "mta_tax" -> num(mtaTax),
      "tip_amount" -> num(tipAmount),
      "tolls_amount" -> num(tollsAmount),
      "improvement_surcharge" -> num(improvementSurcharge),
      "congestion_surcharge" -> num(congestionSurcharge),
      "airport_fee" -> num(airportFee),
      "ehail_fee" -> num(ehailFee),
      "total_amount" -> Json.fromDoubleOrNull(totalAmount),
      "source_file" -> Json.fromString(sourceFile),
      "source_line" -> Json.fromLong(sourceLine),
      "ingested_at" -> Json.fromString(ingestedAt.toString)
    )
  }
}

/** A row that failed parsing or validation, retained for reporting. */
final case class RejectedRow(sourceFile: String, lineNumber: Long, reason: String, detail: String)

/**
 * Maps a raw CSV row onto a [[TripEvent]], applying the TLC domain rules:
 * zone identifiers are bounded by the taxi-zone lookup, trips must move
 * forward in time, and monetary and distance fields must be finite.
 */
private[ingestion] final class TripEventMapper(
    serviceType: String,
    sourceZone: ZoneId
) {

  private val MinZoneId = 1
  private val MaxZoneId = 265
  private val MaxTripDurationSeconds = 24L * 60L * 60L
  private val MaxTripDistanceMiles = 500.0
  private val MaxPlausibleSpeedMph = 120.0

  private val dateTimeFormats: Seq[DateTimeFormatter] = Seq(
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
    DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"),
    DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a")
  )

  def map(
      fields: Vector[String],
      columns: ColumnIndex,
      sourceFile: String,
      lineNumber: Long
  ): Either[RejectedRow, TripEvent] = {

    def reject(reason: String, detail: String): Left[RejectedRow, TripEvent] =
      Left(RejectedRow(sourceFile, lineNumber, reason, detail))

    def rawAt(logical: String): Option[String] =
      columns.optionalIndex(logical).flatMap { idx =>
        if (idx < fields.length) {
          val value = fields(idx).trim
          if (value.isEmpty) None else Some(value)
        } else None
      }

    def doubleAt(logical: String): Option[Double] =
      rawAt(logical).flatMap { raw =>
        try {
          val parsed = raw.toDouble
          if (parsed.isNaN || parsed.isInfinite) None else Some(parsed)
        } catch { case _: NumberFormatException => None }
      }

    def intAt(logical: String): Option[Int] =
      rawAt(logical).flatMap { raw =>
        try Some(raw.toDouble.toInt)
        catch { case _: NumberFormatException => None }
      }

    def timestampAt(logical: String): Option[Instant] =
      rawAt(logical).flatMap { raw =>
        val normalized = if (raw.length > 19 && raw.charAt(19) == '.') raw.substring(0, 19) else raw
        dateTimeFormats.iterator
          .map { formatter =>
            try Some(LocalDateTime.parse(normalized, formatter).atZone(sourceZone).toInstant)
            catch { case _: DateTimeParseException => None }
          }
          .collectFirst { case Some(instant) => instant }
      }

    val pickupTimeOpt = timestampAt("pickup_datetime")
    val dropoffTimeOpt = timestampAt("dropoff_datetime")
    val pickupZoneOpt = intAt("pickup_location_id")
    val dropoffZoneOpt = intAt("dropoff_location_id")
    val distanceOpt = doubleAt("trip_distance")
    val totalOpt = doubleAt("total_amount")

    if (pickupTimeOpt.isEmpty) {
      reject("unparseable_pickup_time", rawAt("pickup_datetime").getOrElse("<empty>"))
    } else if (dropoffTimeOpt.isEmpty) {
      reject("unparseable_dropoff_time", rawAt("dropoff_datetime").getOrElse("<empty>"))
    } else if (pickupZoneOpt.isEmpty || dropoffZoneOpt.isEmpty) {
      reject("missing_zone_id", s"pickup=${rawAt("pickup_location_id").getOrElse("<empty>")} dropoff=${rawAt("dropoff_location_id").getOrElse("<empty>")}")
    } else if (distanceOpt.isEmpty) {
      reject("unparseable_trip_distance", rawAt("trip_distance").getOrElse("<empty>"))
    } else if (totalOpt.isEmpty) {
      reject("unparseable_total_amount", rawAt("total_amount").getOrElse("<empty>"))
    } else {
      val pickupTime = pickupTimeOpt.get
      val dropoffTime = dropoffTimeOpt.get
      val pickupZone = pickupZoneOpt.get
      val dropoffZone = dropoffZoneOpt.get
      val distance = distanceOpt.get
      val total = totalOpt.get
      val durationSeconds = JDuration.between(pickupTime, dropoffTime).getSeconds

      if (pickupZone < MinZoneId || pickupZone > MaxZoneId) {
        reject("zone_id_out_of_range", s"pickup=$pickupZone")
      } else if (dropoffZone < MinZoneId || dropoffZone > MaxZoneId) {
        reject("zone_id_out_of_range", s"dropoff=$dropoffZone")
      } else if (durationSeconds <= 0L) {
        reject("non_positive_duration", s"duration_seconds=$durationSeconds")
      } else if (durationSeconds > MaxTripDurationSeconds) {
        reject("duration_exceeds_limit", s"duration_seconds=$durationSeconds")
      } else if (distance < 0.0 || distance > MaxTripDistanceMiles) {
        reject("distance_out_of_range", s"trip_distance=$distance")
      } else {
        val speed = if (durationSeconds > 0L) Some(distance / (durationSeconds.toDouble / 3600.0)) else None
        val implausibleSpeed = speed.exists(value => value > MaxPlausibleSpeedMph)

        if (implausibleSpeed) {
          reject("implausible_speed", s"speed_mph=${speed.getOrElse(0.0)}")
        } else {
          val vendorId = intAt("vendor_id")
          val eventId = deterministicEventId(vendorId, pickupTime, dropoffTime, pickupZone, dropoffZone, total)

          Right(
            TripEvent(
              eventId = eventId,
              serviceType = serviceType,
              vendorId = vendorId,
              pickupTime = pickupTime,
              dropoffTime = dropoffTime,
              tripDurationSeconds = durationSeconds,
              passengerCount = intAt("passenger_count").filter(_ >= 0),
              tripDistanceMiles = distance,
              averageSpeedMph = speed.map(value => math.rint(value * 100.0) / 100.0),
              pickupLocationId = pickupZone,
              dropoffLocationId = dropoffZone,
              rateCodeId = intAt("ratecode_id"),
              paymentType = intAt("payment_type"),
              storeAndForwardFlag = rawAt("store_and_fwd_flag").map(_.toUpperCase),
              fareAmount = doubleAt("fare_amount"),
              extraAmount = doubleAt("extra"),
              mtaTax = doubleAt("mta_tax"),
              tipAmount = doubleAt("tip_amount"),
              tollsAmount = doubleAt("tolls_amount"),
              improvementSurcharge = doubleAt("improvement_surcharge"),
              congestionSurcharge = doubleAt("congestion_surcharge"),
              airportFee = doubleAt("airport_fee"),
              ehailFee = doubleAt("ehail_fee"),
              totalAmount = total,
              sourceFile = sourceFile,
              sourceLine = lineNumber,
              ingestedAt = Instant.now()
            )
          )
        }
      }
    }
  }

  /**
   * Derives a stable identifier from the natural key of the trip so that a
   * re-run of the same extract produces the same event ids, letting the
   * downstream graph writer deduplicate on replay.
   */
  private def deterministicEventId(
      vendorId: Option[Int],
      pickupTime: Instant,
      dropoffTime: Instant,
      pickupZone: Int,
      dropoffZone: Int,
      totalAmount: Double
  ): String = {
    val canonical = new StringBuilder()
      .append(serviceType).append('|')
      .append(vendorId.getOrElse(-1)).append('|')
      .append(pickupTime.toEpochMilli).append('|')
      .append(dropoffTime.toEpochMilli).append('|')
      .append(pickupZone).append('|')
      .append(dropoffZone).append('|')
      .append(f"$totalAmount%.2f")
      .toString

    UUID.nameUUIDFromBytes(canonical.getBytes(StandardCharsets.UTF_8)).toString
  }
}

/** Thread-safe counters describing a producer run. */
private[ingestion] final class IngestionMetrics {

  private val readRows = new AtomicLong(0L)
  private val acceptedRows = new AtomicLong(0L)
  private val rejectedRows = new AtomicLong(0L)
  private val acknowledged = new AtomicLong(0L)
  private val failed = new AtomicLong(0L)
  private val bytesProduced = new AtomicLong(0L)
  private val rejectionReasons = new java.util.concurrent.ConcurrentHashMap[String, AtomicLong]()

  def recordRead(): Unit = { readRows.incrementAndGet(); () }

  def recordAccepted(payloadBytes: Int): Unit = {
    acceptedRows.incrementAndGet()
    bytesProduced.addAndGet(payloadBytes.toLong)
    ()
  }

  def recordRejected(reason: String): Unit = {
    rejectedRows.incrementAndGet()
    rejectionReasons.computeIfAbsent(reason, _ => new AtomicLong(0L)).incrementAndGet()
    ()
  }

  def recordAcknowledged(): Unit = { acknowledged.incrementAndGet(); () }

  def recordFailure(): Unit = { failed.incrementAndGet(); () }

  def read: Long = readRows.get()
  def accepted: Long = acceptedRows.get()
  def rejected: Long = rejectedRows.get()
  def acked: Long = acknowledged.get()
  def failures: Long = failed.get()
  def bytes: Long = bytesProduced.get()

  def rejectionBreakdown: Seq[(String, Long)] =
    rejectionReasons
      .entrySet()
      .asScala
      .toSeq
      .map(entry => entry.getKey -> entry.getValue.get())
      .sortBy { case (_, count) => -count }
}

/**
 * Throttles emission to a target record rate. A zero target disables pacing
 * entirely, which is the correct mode for bulk backfills into a cold topic.
 */
private[ingestion] final class RatePacer(recordsPerSecond: Int) {

  private val enabled = recordsPerSecond > 0
  private val nanosPerRecord = if (enabled) 1000000000L / recordsPerSecond.toLong else 0L
  private var nextEmitNanos = System.nanoTime()

  def acquire(): Unit = {
    if (enabled) {
      val now = System.nanoTime()
      if (nextEmitNanos < now) {
        nextEmitNanos = now
      }
      val waitNanos = nextEmitNanos - now
      if (waitNanos > 0L) {
        val millis = waitNanos / 1000000L
        val remainder = (waitNanos % 1000000L).toInt
        Thread.sleep(millis, remainder)
      }
      nextEmitNanos += nanosPerRecord
    }
  }
}

/** Enumerates readable CSV sources beneath a file or directory path. */
private[ingestion] object CsvSourceResolver {

  private val SupportedSuffixes = Seq(".csv", ".csv.gz", ".txt", ".txt.gz")

  def resolve(root: Path): Seq[Path] = {
    val candidates =
      if (Files.isDirectory(root)) {
        val stream = Files.list(root)
        try stream.iterator().asScala.toVector
        finally stream.close()
      } else {
        Vector(root)
      }

    candidates
      .filter(Files.isRegularFile(_))
      .filter(path => SupportedSuffixes.exists(suffix => path.getFileName.toString.toLowerCase.endsWith(suffix)))
      .sortBy(_.getFileName.toString)
  }

  def open(path: Path): BufferedReader = {
    val raw: InputStream = new FileInputStream(path.toFile)
    val decoded: InputStream =
      if (path.getFileName.toString.toLowerCase.endsWith(".gz")) new GZIPInputStream(raw, 65536) else raw
    new BufferedReader(new InputStreamReader(decoded, StandardCharsets.UTF_8), 262144)
  }
}

/** Raised when the producer cannot continue and the run must be aborted. */
final class IngestionAbortedException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

/**
 * Reads TLC trip CSV extracts and publishes canonical JSON events to Kafka.
 * The producer is idempotent at the broker level and deterministic at the
 * payload level, so an interrupted run can simply be restarted.
 */
final class TaxiTripProducer(settings: ProducerSettings) extends StrictLogging {

  private val metrics = new IngestionMetrics
  private val running = new AtomicBoolean(true)
  private val mapper = new TripEventMapper(settings.serviceType, settings.sourceTimeZone)
  private val pacer = new RatePacer(settings.recordsPerSecond)
  private val loggedFailures = new AtomicLong(0L)
  private val MaxLoggedFailures = 25L

  private val sendCallback: Callback = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception == null) {
        metrics.recordAcknowledged()
      } else {
        metrics.recordFailure()
        if (loggedFailures.incrementAndGet() <= MaxLoggedFailures) {
          logger.error(s"Delivery failed for topic ${settings.topic}: ${exception.getMessage}", exception)
        }
      }
    }
  }

  def requestShutdown(): Unit = {
    if (running.compareAndSet(true, false)) {
      logger.warn("Shutdown requested; draining in-flight records")
    }
  }

  def run(): Int = {
    val sources = CsvSourceResolver.resolve(settings.input)
    if (sources.isEmpty) {
      throw new IngestionAbortedException(s"No readable CSV sources found under ${settings.input}")
    }

    logger.info(
      s"Starting ingestion: files=${sources.size} topic=${settings.topic} " +
        s"brokers=${settings.bootstrapServers} service=${settings.serviceType} " +
        s"rate=${if (settings.recordsPerSecond == 0) "unthrottled" else settings.recordsPerSecond.toString}"
    )

    val producer = new KafkaProducer[String, String](buildProducerProperties())
    val reporter = startReporter()
    val startNanos = System.nanoTime()

    try {
      val iterator = sources.iterator
      while (iterator.hasNext && running.get()) {
        val source = iterator.next()
        ingestFile(source, producer)
      }
      producer.flush()
      0
    } catch {
      case aborted: IngestionAbortedException =>
        logger.error(s"Ingestion aborted: ${aborted.getMessage}")
        producer.flush()
        2
      case NonFatal(error) =>
        logger.error(s"Ingestion failed: ${error.getMessage}", error)
        producer.flush()
        2
    } finally {
      reporter.shutdownNow()
      closeQuietly(producer)
      logSummary(System.nanoTime() - startNanos)
    }
  }

  private def ingestFile(source: Path, producer: KafkaProducer[String, String]): Unit = {
    val fileName = source.getFileName.toString
    val reader = CsvSourceResolver.open(source)
    var lineNumber = 0L

    try {
      val headerLine = reader.readLine()
      if (headerLine == null) {
        logger.warn(s"Skipping empty source: $fileName")
        return
      }
      lineNumber += 1L

      val columns = ColumnIndex.fromHeader(CsvLineParser.parse(stripByteOrderMark(headerLine), ','))
      val missing = columns.missing(ColumnIndex.RequiredColumns)
      if (missing.nonEmpty) {
        throw new IngestionAbortedException(
          s"Source $fileName is missing required columns: ${missing.mkString(", ")}"
        )
      }

      logger.info(s"Processing source: $fileName")

      var line = reader.readLine()
      while (line != null && running.get() && !limitReached) {
        lineNumber += 1L
        if (line.trim.nonEmpty) {
          metrics.recordRead()
          val fields = CsvLineParser.parse(line, ',')
          mapper.map(fields, columns, fileName, lineNumber) match {
            case Right(event) =>
              emit(producer, event)
            case Left(rejected) =>
              metrics.recordRejected(rejected.reason)
              if (settings.failFast) {
                throw new IngestionAbortedException(
                  s"Rejected row at $fileName:${rejected.lineNumber} (${rejected.reason}: ${rejected.detail})"
                )
              }
          }
        }
        line = reader.readLine()
      }
    } finally {
      closeQuietly(reader)
    }
  }

  private def emit(producer: KafkaProducer[String, String], event: TripEvent): Unit = {
    pacer.acquire()

    val payload = event.asJson.noSpaces
    val record = new ProducerRecord[String, String](
      settings.topic,
      null,
      java.lang.Long.valueOf(event.pickupTime.toEpochMilli),
      event.partitionKey,
      payload
    )

    val headers = record.headers()
    headers.add(new RecordHeader("event-id", event.eventId.getBytes(StandardCharsets.UTF_8)))
    headers.add(new RecordHeader("service-type", event.serviceType.getBytes(StandardCharsets.UTF_8)))
    headers.add(new RecordHeader("source-file", event.sourceFile.getBytes(StandardCharsets.UTF_8)))
    headers.add(new RecordHeader("schema-version", "1".getBytes(StandardCharsets.UTF_8)))

    val _ = producer.send(record, sendCallback)
    metrics.recordAccepted(payload.length)
  }

  private def limitReached: Boolean =
    settings.maxRecords > 0L && metrics.accepted >= settings.maxRecords

  private def buildProducerProperties(): Properties = {
    val props = new Properties()
    props.put(KafkaProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers)
    props.put(KafkaProducerConfig.CLIENT_ID_CONFIG, settings.clientId)
    props.put(KafkaProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(KafkaProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(KafkaProducerConfig.ACKS_CONFIG, "all")
    props.put(KafkaProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, java.lang.Boolean.TRUE)
    props.put(KafkaProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.valueOf(5))
    props.put(KafkaProducerConfig.RETRIES_CONFIG, Integer.valueOf(Int.MaxValue))
    props.put(KafkaProducerConfig.COMPRESSION_TYPE_CONFIG, settings.compressionType)
    props.put(KafkaProducerConfig.LINGER_MS_CONFIG, Integer.valueOf(settings.lingerMs))
    props.put(KafkaProducerConfig.BATCH_SIZE_CONFIG, Integer.valueOf(settings.batchSizeBytes))
    props.put(KafkaProducerConfig.BUFFER_MEMORY_CONFIG, java.lang.Long.valueOf(settings.bufferMemoryBytes))
    props.put(KafkaProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.valueOf(settings.requestTimeoutMs))
    props.put(KafkaProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.valueOf(settings.deliveryTimeoutMs))
    props.put(KafkaProducerConfig.MAX_BLOCK_MS_CONFIG, java.lang.Long.valueOf(60000L))
    props.put(KafkaProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, java.lang.Long.valueOf(10000L))
    props
  }

  private def startReporter(): ScheduledExecutorService = {
    val executor = Executors.newSingleThreadScheduledExecutor(new java.util.concurrent.ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable, "taxi-trip-producer-reporter")
        thread.setDaemon(true)
        thread
      }
    })

    val startNanos = System.nanoTime()
    val task = new Runnable {
      override def run(): Unit = {
        val elapsedSeconds = math.max(1L, (System.nanoTime() - startNanos) / 1000000000L)
        logger.info(
          f"progress read=${metrics.read}%d accepted=${metrics.accepted}%d rejected=${metrics.rejected}%d " +
            f"acked=${metrics.acked}%d failed=${metrics.failures}%d " +
            f"throughput=${metrics.accepted.toDouble / elapsedSeconds.toDouble}%.1f rec/s"
        )
      }
    }

    val _ = executor.scheduleAtFixedRate(
      task,
      settings.reportIntervalSeconds.toLong,
      settings.reportIntervalSeconds.toLong,
      TimeUnit.SECONDS
    )
    executor
  }

  private def logSummary(elapsedNanos: Long): Unit = {
    val elapsedSeconds = math.max(1L, elapsedNanos / 1000000000L)
    logger.info(
      f"ingestion summary: read=${metrics.read}%d accepted=${metrics.accepted}%d " +
        f"rejected=${metrics.rejected}%d acknowledged=${metrics.acked}%d failed=${metrics.failures}%d " +
        f"payload_bytes=${metrics.bytes}%d elapsed_seconds=$elapsedSeconds%d " +
        f"mean_throughput=${metrics.accepted.toDouble / elapsedSeconds.toDouble}%.1f rec/s"
    )

    val breakdown = metrics.rejectionBreakdown
    if (breakdown.nonEmpty) {
      logger.warn(s"rejection breakdown: ${breakdown.map { case (reason, count) => s"$reason=$count" }.mkString(" ")}")
    }
  }

  private def closeQuietly(producer: KafkaProducer[String, String]): Unit =
    try producer.close(JDuration.ofMillis(settings.deliveryTimeoutMs.toLong))
    catch { case NonFatal(error) => logger.warn(s"Producer close was not clean: ${error.getMessage}") }

  private def closeQuietly(reader: BufferedReader): Unit =
    try reader.close()
    catch { case NonFatal(error) => logger.warn(s"Reader close was not clean: ${error.getMessage}") }

  private def stripByteOrderMark(line: String): String =
    if (line.nonEmpty && line.charAt(0) == '\uFEFF') line.substring(1) else line

  def failureCount: Long = metrics.failures
}

object TaxiTripProducer extends StrictLogging {

  def main(args: Array[String]): Unit = {
    ProducerSettings.parse(args) match {
      case Left(message) =>
        Console.err.println(message)
        if (message == ProducerSettings.Usage) sys.exit(0) else sys.exit(1)

      case Right(settings) =>
        val producer = new TaxiTripProducer(settings)
        val terminated = new CountDownLatch(1)

        Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
          override def run(): Unit = {
            producer.requestShutdown()
            val _ = terminated.await(60L, TimeUnit.SECONDS)
          }
        }, "taxi-trip-producer-shutdown"))

        val exitCode =
          try {
            val result = producer.run()
            if (result == 0 && producer.failureCount > 0L) 2 else result
          } catch {
            case NonFatal(error) =>
              logger.error(s"Unrecoverable producer error: ${error.getMessage}", error)
              2
          } finally {
            terminated.countDown()
          }

        sys.exit(exitCode)
    }
  }
}
