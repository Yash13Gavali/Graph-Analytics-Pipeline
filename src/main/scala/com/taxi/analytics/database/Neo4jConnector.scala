package com.taxi.analytics.database

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, ThreadLocalRandom, TimeUnit}
import java.util.function.{Function => JFunction}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, LinkedHashSet => JLinkedHashSet, List => JList, Map => JMap, Set => JSet}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.typesafe.scalalogging.StrictLogging
import org.neo4j.driver.exceptions.{ClientException, Neo4jException, ServiceUnavailableException, SessionExpiredException, TransientException}
import org.neo4j.driver.summary.{ResultSummary, SummaryCounters}
import org.neo4j.driver.{AccessMode, AuthTokens, Bookmark, Config, ConnectionPoolMetrics, Driver, GraphDatabase, Logging, Record, Session, SessionConfig, TransactionCallback, TransactionContext}

// ---------------------------------------------------------------------------
// Real-Time Graph Analytics & NYC Taxi Demand Forecasting
// Neo4j graph data access layer
//
// A single connector instance owns one driver and therefore one connection
// pool. Instances are safe to share across threads and are obtained through
// the companion registry so that a Spark executor reuses one pool for every
// task it runs rather than opening a driver per partition.
// ---------------------------------------------------------------------------

/** Backoff policy applied around driver-level transaction retries. */
final case class RetryPolicy(
    maxAttempts: Int,
    initialBackoffMillis: Long,
    maxBackoffMillis: Long,
    backoffMultiplier: Double,
    jitterFactor: Double
) extends Serializable {

  require(maxAttempts >= 1, "maxAttempts must be at least 1")
  require(initialBackoffMillis >= 0L, "initialBackoffMillis must be non-negative")
  require(maxBackoffMillis >= initialBackoffMillis, "maxBackoffMillis must be >= initialBackoffMillis")
  require(backoffMultiplier >= 1.0, "backoffMultiplier must be >= 1.0")
  require(jitterFactor >= 0.0 && jitterFactor <= 1.0, "jitterFactor must be within [0.0, 1.0]")

  /** Full-jitter exponential backoff for the given one-based attempt number. */
  def backoffFor(attempt: Int): Long = {
    val exponential = initialBackoffMillis * math.pow(backoffMultiplier, (attempt - 1).toDouble)
    val capped = math.min(maxBackoffMillis.toDouble, exponential).toLong
    if (jitterFactor <= 0.0 || capped <= 0L) {
      capped
    } else {
      val jitterSpan = (capped.toDouble * jitterFactor).toLong
      val offset = if (jitterSpan <= 0L) 0L else ThreadLocalRandom.current().nextLong(-jitterSpan, jitterSpan + 1L)
      math.max(0L, capped + offset)
    }
  }
}

object RetryPolicy {
  val Default: RetryPolicy = RetryPolicy(
    maxAttempts = 5,
    initialBackoffMillis = 250L,
    maxBackoffMillis = 15000L,
    backoffMultiplier = 2.0,
    jitterFactor = 0.2
  )
}

/** Connection and execution parameters for the graph store. */
final case class Neo4jConfig(
    uri: String,
    user: String,
    password: String,
    database: String,
    maxConnectionPoolSize: Int,
    connectionAcquisitionTimeoutSeconds: Int,
    connectionTimeoutSeconds: Int,
    maxConnectionLifetimeMinutes: Int,
    connectionLivenessCheckSeconds: Int,
    maxTransactionRetrySeconds: Int,
    fetchSize: Int,
    writeBatchSize: Int,
    userAgent: String,
    encrypted: Boolean,
    driverMetricsEnabled: Boolean,
    trackBookmarks: Boolean,
    retryPolicy: RetryPolicy
) extends Serializable {

  require(uri.trim.nonEmpty, "uri must not be empty")
  require(database.trim.nonEmpty, "database must not be empty")
  require(maxConnectionPoolSize >= 1, "maxConnectionPoolSize must be at least 1")
  require(writeBatchSize >= 1, "writeBatchSize must be at least 1")
  require(fetchSize >= 1, "fetchSize must be at least 1")

  /**
   * Identity of the underlying pool. Two configs that address the same
   * database as the same principal share a driver; anything else does not.
   */
  def instanceKey: String = s"$uri|$database|$user"

  /** URI schemes that negotiate TLS themselves reject explicit encryption settings. */
  def schemeManagesEncryption: Boolean = {
    val scheme = uri.trim.toLowerCase
    scheme.startsWith("bolt+s") || scheme.startsWith("bolt+ssc") ||
      scheme.startsWith("neo4j+s") || scheme.startsWith("neo4j+ssc")
  }

  /** Redacted rendering, safe for logs. */
  def describe: String = s"uri=$uri database=$database user=$user pool=$maxConnectionPoolSize"
}

object Neo4jConfig {

  val DefaultUri = "bolt://neo4j:7687"
  val DefaultUser = "neo4j"
  val DefaultDatabase = "neo4j"
  val DefaultUserAgent = "nyc-taxi-graph-analytics"

  /**
   * Builds a configuration from the environment. The password is only read
   * from the environment so that it never appears in a process command line.
   */
  def fromEnvironment(): Either[String, Neo4jConfig] = {
    def env(key: String): Option[String] = Option(System.getenv(key)).map(_.trim).filter(_.nonEmpty)

    def intEnv(key: String, default: Int, minimum: Int): Either[String, Int] =
      env(key) match {
        case None => Right(default)
        case Some(raw) =>
          try {
            val value = raw.toInt
            if (value < minimum) Left(s"$key must be >= $minimum") else Right(value)
          } catch { case _: NumberFormatException => Left(s"$key expects an integer, got '$raw'") }
      }

    def boolEnv(key: String, default: Boolean): Either[String, Boolean] =
      env(key) match {
        case None => Right(default)
        case Some(raw) =>
          raw.toLowerCase match {
            case "true" | "yes" | "1" => Right(true)
            case "false" | "no" | "0" => Right(false)
            case other => Left(s"$key expects a boolean, got '$other'")
          }
      }

    for {
      password <- env("NEO4J_PASSWORD").toRight("NEO4J_PASSWORD must be set")
      poolSize <- intEnv("NEO4J_MAX_POOL_SIZE", 32, 1)
      acquisitionTimeout <- intEnv("NEO4J_ACQUISITION_TIMEOUT_SECONDS", 60, 1)
      connectionTimeout <- intEnv("NEO4J_CONNECTION_TIMEOUT_SECONDS", 30, 1)
      connectionLifetime <- intEnv("NEO4J_CONNECTION_LIFETIME_MINUTES", 60, 1)
      livenessCheck <- intEnv("NEO4J_LIVENESS_CHECK_SECONDS", 60, 1)
      retryWindow <- intEnv("NEO4J_TRANSACTION_RETRY_SECONDS", 30, 1)
      fetchSize <- intEnv("NEO4J_FETCH_SIZE", 1000, 1)
      batchSize <- intEnv("NEO4J_WRITE_BATCH_SIZE", 2000, 1)
      maxAttempts <- intEnv("NEO4J_MAX_WRITE_ATTEMPTS", RetryPolicy.Default.maxAttempts, 1)
      encrypted <- boolEnv("NEO4J_ENCRYPTED", default = false)
      metricsEnabled <- boolEnv("NEO4J_DRIVER_METRICS", default = true)
      trackBookmarks <- boolEnv("NEO4J_TRACK_BOOKMARKS", default = true)
    } yield Neo4jConfig(
      uri = env("NEO4J_URI").getOrElse(DefaultUri),
      user = env("NEO4J_USER").getOrElse(DefaultUser),
      password = password,
      database = env("NEO4J_DATABASE").getOrElse(DefaultDatabase),
      maxConnectionPoolSize = poolSize,
      connectionAcquisitionTimeoutSeconds = acquisitionTimeout,
      connectionTimeoutSeconds = connectionTimeout,
      maxConnectionLifetimeMinutes = connectionLifetime,
      connectionLivenessCheckSeconds = livenessCheck,
      maxTransactionRetrySeconds = retryWindow,
      fetchSize = fetchSize,
      writeBatchSize = batchSize,
      userAgent = env("NEO4J_USER_AGENT").getOrElse(DefaultUserAgent),
      encrypted = encrypted,
      driverMetricsEnabled = metricsEnabled,
      trackBookmarks = trackBookmarks,
      retryPolicy = RetryPolicy.Default.copy(maxAttempts = maxAttempts)
    )
  }
}

/** Converts Scala values into Bolt-compatible parameter objects. */
object ParameterEncoder {

  def encodeMap(parameters: Map[String, Any]): JMap[String, Object] = {
    val encoded: JMap[String, Object] = new JHashMap[String, Object](math.max(4, parameters.size * 2))
    parameters.foreach { case (key, value) =>
      val _ = encoded.put(key, encode(value))
    }
    encoded
  }

  def encodeRows(rows: Seq[Map[String, Any]]): JList[JMap[String, Object]] = {
    val encoded: JList[JMap[String, Object]] = new JArrayList[JMap[String, Object]](rows.size)
    rows.foreach(row => encoded.add(encodeMap(row)))
    encoded
  }

  def encode(value: Any): Object = value match {
    case null => null
    case None => null
    case Some(inner) => encode(inner)
    case v: String => v
    case v: Boolean => java.lang.Boolean.valueOf(v)
    case v: Byte => java.lang.Long.valueOf(v.toLong)
    case v: Short => java.lang.Long.valueOf(v.toLong)
    case v: Int => java.lang.Long.valueOf(v.toLong)
    case v: Long => java.lang.Long.valueOf(v)
    case v: Float => java.lang.Double.valueOf(v.toDouble)
    case v: Double => java.lang.Double.valueOf(v)
    case v: BigDecimal => java.lang.Double.valueOf(v.toDouble)
    case v: BigInt => java.lang.Long.valueOf(v.toLong)
    case v: Map[_, _] =>
      val nested: JMap[String, Object] = new JHashMap[String, Object](math.max(4, v.size * 2))
      v.foreach { case (key, nestedValue) =>
        val _ = nested.put(String.valueOf(key), encode(nestedValue))
      }
      nested
    case v: Iterable[_] =>
      val list: JList[Object] = new JArrayList[Object](v.size)
      v.foreach(element => list.add(encode(element)))
      list
    case v: Array[_] =>
      val list: JList[Object] = new JArrayList[Object](v.length)
      v.foreach(element => list.add(encode(element)))
      list
    case other: Object => other
    case other => String.valueOf(other)
  }
}

/** Aggregated write counters returned to callers. */
final case class WriteOutcome(
    statementsExecuted: Long,
    rowsSubmitted: Long,
    nodesCreated: Long,
    nodesDeleted: Long,
    relationshipsCreated: Long,
    relationshipsDeleted: Long,
    propertiesSet: Long,
    labelsAdded: Long,
    labelsRemoved: Long,
    retries: Long,
    elapsedMillis: Long
) {

  def merge(other: WriteOutcome): WriteOutcome = WriteOutcome(
    statementsExecuted = statementsExecuted + other.statementsExecuted,
    rowsSubmitted = rowsSubmitted + other.rowsSubmitted,
    nodesCreated = nodesCreated + other.nodesCreated,
    nodesDeleted = nodesDeleted + other.nodesDeleted,
    relationshipsCreated = relationshipsCreated + other.relationshipsCreated,
    relationshipsDeleted = relationshipsDeleted + other.relationshipsDeleted,
    propertiesSet = propertiesSet + other.propertiesSet,
    labelsAdded = labelsAdded + other.labelsAdded,
    labelsRemoved = labelsRemoved + other.labelsRemoved,
    retries = retries + other.retries,
    elapsedMillis = elapsedMillis + other.elapsedMillis
  )

  def summary: String =
    s"statements=$statementsExecuted rows=$rowsSubmitted nodes_created=$nodesCreated " +
      s"rels_created=$relationshipsCreated props_set=$propertiesSet retries=$retries elapsed_ms=$elapsedMillis"
}

object WriteOutcome {

  val Empty: WriteOutcome = WriteOutcome(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L)

  def from(summary: ResultSummary, rows: Long, retries: Long, elapsedMillis: Long): WriteOutcome = {
    val counters: SummaryCounters = summary.counters()
    WriteOutcome(
      statementsExecuted = 1L,
      rowsSubmitted = rows,
      nodesCreated = counters.nodesCreated().toLong,
      nodesDeleted = counters.nodesDeleted().toLong,
      relationshipsCreated = counters.relationshipsCreated().toLong,
      relationshipsDeleted = counters.relationshipsDeleted().toLong,
      propertiesSet = counters.propertiesSet().toLong,
      labelsAdded = counters.labelsAdded().toLong,
      labelsRemoved = counters.labelsRemoved().toLong,
      retries = retries,
      elapsedMillis = elapsedMillis
    )
  }
}

/** Lifetime counters for a connector instance. */
final class ConnectorMetrics {

  private val sessionsOpened = new AtomicLong(0L)
  private val readsExecuted = new AtomicLong(0L)
  private val writesExecuted = new AtomicLong(0L)
  private val rowsWritten = new AtomicLong(0L)
  private val retriesPerformed = new AtomicLong(0L)
  private val failures = new AtomicLong(0L)
  private val writeNanos = new AtomicLong(0L)

  private[database] def recordSessionOpened(): Unit = { sessionsOpened.incrementAndGet(); () }
  private[database] def recordRead(): Unit = { readsExecuted.incrementAndGet(); () }
  private[database] def recordRetry(): Unit = { retriesPerformed.incrementAndGet(); () }
  private[database] def recordFailure(): Unit = { failures.incrementAndGet(); () }

  private[database] def recordWrite(rows: Long, elapsedNanos: Long): Unit = {
    writesExecuted.incrementAndGet()
    rowsWritten.addAndGet(rows)
    writeNanos.addAndGet(elapsedNanos)
    ()
  }

  def sessions: Long = sessionsOpened.get()
  def reads: Long = readsExecuted.get()
  def writes: Long = writesExecuted.get()
  def rows: Long = rowsWritten.get()
  def retries: Long = retriesPerformed.get()
  def failureCount: Long = failures.get()
  def writeMillis: Long = writeNanos.get() / 1000000L

  def snapshot: String =
    s"sessions=$sessions reads=$reads writes=$writes rows=$rows retries=$retries " +
      s"failures=$failureCount write_ms=$writeMillis"
}

/** Raised when a statement cannot be completed within the retry budget. */
final class Neo4jExecutionException(message: String, cause: Throwable)
    extends RuntimeException(message, cause)

/**
 * Thread-safe access layer over a single Neo4j driver.
 *
 * Sessions are short-lived and always closed; the driver's connection pool is
 * the durable resource. Writes go through managed transactions, which the
 * driver already retries for leader elections and deadlocks; the additional
 * outer loop here covers the cases the driver gives up on, such as a broker
 * restart that outlasts the driver's own retry window.
 */
final class Neo4jConnector(val config: Neo4jConfig) extends AutoCloseable with StrictLogging {

  val metrics = new ConnectorMetrics

  private val bookmarks = new AtomicReference[JSet[Bookmark]](new JLinkedHashSet[Bookmark]())

  private val driver: Driver = {
    val builder = Config.builder()
      .withMaxConnectionPoolSize(config.maxConnectionPoolSize)
      .withConnectionAcquisitionTimeout(config.connectionAcquisitionTimeoutSeconds.toLong, TimeUnit.SECONDS)
      .withConnectionTimeout(config.connectionTimeoutSeconds.toLong, TimeUnit.SECONDS)
      .withMaxConnectionLifetime(config.maxConnectionLifetimeMinutes.toLong, TimeUnit.MINUTES)
      .withConnectionLivenessCheckTimeout(config.connectionLivenessCheckSeconds.toLong, TimeUnit.SECONDS)
      .withMaxTransactionRetryTime(config.maxTransactionRetrySeconds.toLong, TimeUnit.SECONDS)
      .withFetchSize(config.fetchSize.toLong)
      .withUserAgent(config.userAgent)
      .withLogging(Logging.slf4j())

    val withMetrics = if (config.driverMetricsEnabled) builder.withDriverMetrics() else builder.withoutDriverMetrics()

    // Schemes such as neo4j+s negotiate TLS themselves and reject an explicit
    // encryption setting, so only plain schemes are configured here.
    val withEncryption =
      if (config.schemeManagesEncryption) withMetrics
      else if (config.encrypted) withMetrics.withEncryption()
      else withMetrics.withoutEncryption()

    GraphDatabase.driver(
      config.uri,
      AuthTokens.basic(config.user, config.password),
      withEncryption.build()
    )
  }

  /** Blocks until the driver can reach the server, or throws. */
  def verifyConnectivity(): Unit = {
    driver.verifyConnectivity()
    logger.info(s"Neo4j connector ready: ${config.describe}")
  }

  /** Cheap liveness probe suitable for a health endpoint. */
  def ping(): Boolean =
    try {
      val result = readOne("RETURN 1 AS ok", Map.empty)(record => record.get("ok").asLong())
      result.contains(1L)
    } catch {
      case NonFatal(error) =>
        logger.warn(s"Neo4j ping failed: ${error.getMessage}")
        false
    }

  // -------------------------------------------------------------------------
  // Session lifecycle
  // -------------------------------------------------------------------------

  /** Runs `body` against a write session and always closes it. */
  def withWriteSession[T](body: Session => T): T = withSession(AccessMode.WRITE)(body)

  /** Runs `body` against a read session, routable to a follower in a cluster. */
  def withReadSession[T](body: Session => T): T = withSession(AccessMode.READ)(body)

  private def withSession[T](mode: AccessMode)(body: Session => T): T = {
    val session = driver.session(sessionConfig(mode))
    metrics.recordSessionOpened()
    try {
      val result = body(session)
      if (config.trackBookmarks) {
        captureBookmarks(session)
      }
      result
    } finally {
      session.close()
    }
  }

  private def sessionConfig(mode: AccessMode): SessionConfig = {
    val builder = SessionConfig.builder()
      .withDatabase(config.database)
      .withDefaultAccessMode(mode)
      .withFetchSize(config.fetchSize.toLong)

    if (config.trackBookmarks) {
      val current = bookmarks.get()
      if (!current.isEmpty) {
        val _ = builder.withBookmarks(new JArrayList[Bookmark](current))
      }
    }

    builder.build()
  }

  /**
   * Retains the bookmarks of the session just closed so that a subsequent
   * read observes the writes that preceded it, even when routed elsewhere.
   */
  private def captureBookmarks(session: Session): Unit = {
    val latest = session.lastBookmarks()
    if (latest != null && !latest.isEmpty) {
      val replacement: JSet[Bookmark] = new JLinkedHashSet[Bookmark](latest)
      bookmarks.set(replacement)
    }
  }

  // -------------------------------------------------------------------------
  // Reads
  // -------------------------------------------------------------------------

  /** Executes a read query and materialises every record through `mapper`. */
  def read[T](cypher: String, parameters: Map[String, Any] = Map.empty)(mapper: Record => T): Vector[T] = {
    val encoded = ParameterEncoder.encodeMap(parameters)

    val records = withReadSession { session =>
      session.executeRead(new TransactionCallback[JList[Record]] {
        override def execute(tx: TransactionContext): JList[Record] = tx.run(cypher, encoded).list()
      })
    }

    metrics.recordRead()
    records.asScala.toVector.map(mapper)
  }

  /** Executes a read query expected to yield at most one record. */
  def readOne[T](cypher: String, parameters: Map[String, Any] = Map.empty)(mapper: Record => T): Option[T] =
    read(cypher, parameters)(mapper).headOption

  // -------------------------------------------------------------------------
  // Writes
  // -------------------------------------------------------------------------

  /**
   * Executes a single write statement in a managed transaction, retrying
   * transient failures according to the configured policy.
   */
  def write(cypher: String, parameters: Map[String, Any] = Map.empty): WriteOutcome =
    executeWrite(cypher, ParameterEncoder.encodeMap(parameters), rowCount = 1L)

  /**
   * Executes `cypher` once per chunk of `rows`, binding each chunk to the
   * `rows` parameter. The statement is expected to open with
   * `UNWIND $rows AS row`, which keeps the round-trip count proportional to
   * the number of chunks rather than the number of rows.
   */
  def writeBatch(
      cypher: String,
      rows: Iterator[Map[String, Any]],
      batchSize: Int = config.writeBatchSize,
      parameterName: String = "rows"
  ): WriteOutcome = {
    require(batchSize >= 1, "batchSize must be at least 1")

    var aggregate = WriteOutcome.Empty
    rows.grouped(batchSize).foreach { chunk =>
      val encoded: JMap[String, Object] = new JHashMap[String, Object](2)
      val _ = encoded.put(parameterName, ParameterEncoder.encodeRows(chunk).asInstanceOf[Object])
      aggregate = aggregate.merge(executeWrite(cypher, encoded, chunk.size.toLong))
    }
    aggregate
  }

  /**
   * Batch variant for callers that already hold Bolt-typed parameter maps,
   * which avoids a second encoding pass on hot write paths such as a Spark
   * `foreachPartition` sink.
   */
  def writeEncodedBatch(
      cypher: String,
      rows: Iterator[JMap[String, Object]],
      batchSize: Int = config.writeBatchSize,
      parameterName: String = "rows"
  ): WriteOutcome = {
    require(batchSize >= 1, "batchSize must be at least 1")

    var aggregate = WriteOutcome.Empty
    rows.grouped(batchSize).foreach { chunk =>
      val payload: JList[JMap[String, Object]] = new JArrayList[JMap[String, Object]](chunk.size)
      chunk.foreach(row => payload.add(row))
      val encoded: JMap[String, Object] = new JHashMap[String, Object](2)
      val _ = encoded.put(parameterName, payload.asInstanceOf[Object])
      aggregate = aggregate.merge(executeWrite(cypher, encoded, chunk.size.toLong))
    }
    aggregate
  }

  /**
   * Runs several statements inside one managed transaction, so that either
   * all of them commit or none do.
   */
  def writeTransaction[T](body: TransactionContext => T): T = {
    val started = System.nanoTime()
    val result = runWithRetry("write-transaction") { retryCount =>
      val _ = retryCount
      withWriteSession { session =>
        session.executeWrite(new TransactionCallback[AnyRef] {
          override def execute(tx: TransactionContext): AnyRef = body(tx).asInstanceOf[AnyRef]
        })
      }
    }
    metrics.recordWrite(0L, System.nanoTime() - started)
    result.asInstanceOf[T]
  }

  /**
   * Applies schema statements one transaction at a time. Constraint and index
   * definitions cannot share a transaction with data operations, so they are
   * deliberately not batched.
   */
  def applySchema(statements: Seq[String]): Int = {
    statements.foreach { statement =>
      val _ = executeWrite(statement, new JHashMap[String, Object](0), rowCount = 0L)
    }
    logger.info(s"Applied ${statements.size} schema statements to database ${config.database}")
    statements.size
  }

  /** Blocks until every index in the database is online. */
  def awaitIndexes(timeoutSeconds: Int): Unit = {
    val _ = write("CALL db.awaitIndexes($timeout)", Map("timeout" -> timeoutSeconds))
  }

  private def executeWrite(cypher: String, parameters: JMap[String, Object], rowCount: Long): WriteOutcome = {
    val started = System.nanoTime()

    val summary = runWithRetry(abbreviate(cypher)) { _ =>
      withWriteSession { session =>
        session.executeWrite(new TransactionCallback[ResultSummary] {
          override def execute(tx: TransactionContext): ResultSummary = tx.run(cypher, parameters).consume()
        })
      }
    }

    val elapsedNanos = System.nanoTime() - started
    metrics.recordWrite(rowCount, elapsedNanos)
    WriteOutcome.from(summary, rowCount, retriesOf(cypher), elapsedNanos / 1000000L)
  }

  // -------------------------------------------------------------------------
  // Retry handling
  // -------------------------------------------------------------------------

  private val retryCounters = new ConcurrentHashMap[String, AtomicLong]()

  private def retriesOf(cypher: String): Long =
    Option(retryCounters.get(abbreviate(cypher))).map(_.get()).getOrElse(0L)

  private def recordStatementRetry(label: String): Unit = {
    val _ = retryCounters
      .computeIfAbsent(label, new JFunction[String, AtomicLong] {
        override def apply(key: String): AtomicLong = new AtomicLong(0L)
      })
      .incrementAndGet()
    metrics.recordRetry()
  }

  private def runWithRetry[T](label: String)(body: Int => T): T = {
    val policy = config.retryPolicy
    var attempt = 1
    var lastError: Throwable = null
    var outcome: Option[T] = None

    while (outcome.isEmpty && attempt <= policy.maxAttempts) {
      try {
        outcome = Some(body(attempt))
      } catch {
        case error: Throwable if isRetryable(error) =>
          lastError = error
          if (attempt >= policy.maxAttempts) {
            metrics.recordFailure()
          } else {
            recordStatementRetry(label)
            val backoff = policy.backoffFor(attempt)
            logger.warn(s"Retryable failure on [$label] attempt $attempt: ${error.getMessage}; retrying in ${backoff}ms")
            Thread.sleep(backoff)
          }
          attempt += 1

        case NonFatal(error) =>
          metrics.recordFailure()
          throw new Neo4jExecutionException(s"Non-retryable failure on [$label]: ${error.getMessage}", error)
      }
    }

    outcome.getOrElse(
      throw new Neo4jExecutionException(
        s"Statement [$label] failed after ${policy.maxAttempts} attempts",
        lastError
      )
    )
  }

  /**
   * Classifies failures worth another attempt. Client errors such as a syntax
   * fault or a constraint violation are deliberately excluded: retrying them
   * only multiplies the load without changing the result.
   */
  private def isRetryable(error: Throwable): Boolean = error match {
    case _: ServiceUnavailableException => true
    case _: SessionExpiredException => true
    case _: TransientException => true
    case client: ClientException => Option(client.code()).exists(_.startsWith("Neo.TransientError"))
    case neo4j: Neo4jException => Option(neo4j.code()).exists(_.startsWith("Neo.TransientError"))
    case _: java.net.SocketTimeoutException => true
    case _: java.net.ConnectException => true
    case _ => false
  }

  private def abbreviate(cypher: String): String = {
    val collapsed = cypher.replaceAll("\\s+", " ").trim
    if (collapsed.length <= 96) collapsed else s"${collapsed.take(93)}..."
  }

  // -------------------------------------------------------------------------
  // Introspection and shutdown
  // -------------------------------------------------------------------------

  /** Current pool utilisation, empty unless driver metrics are enabled. */
  def poolUtilisation: Seq[String] =
    if (!config.driverMetricsEnabled) {
      Seq.empty
    } else {
      driver.metrics().connectionPoolMetrics().asScala.toSeq.map { pool: ConnectionPoolMetrics =>
        s"pool=${pool.id()} in_use=${pool.inUse()} idle=${pool.idle()} created=${pool.created()} " +
          s"failed_to_create=${pool.failedToCreate()} timed_out=${pool.timedOutToAcquire()}"
      }
    }

  def logMetrics(): Unit = {
    logger.info(s"connector metrics: ${metrics.snapshot}")
    poolUtilisation.foreach(line => logger.info(s"connection $line"))
  }

  override def close(): Unit = {
    try {
      logMetrics()
      driver.close()
      logger.info(s"Neo4j connector closed: ${config.describe}")
    } catch {
      case NonFatal(error) => logger.warn(s"Neo4j connector close was not clean: ${error.getMessage}")
    }
  }
}

/**
 * Registry of connector instances keyed by target database. A Spark executor
 * calls [[shared]] from every task and receives the same pooled connector for
 * the lifetime of the JVM.
 */
object Neo4jConnector extends StrictLogging {

  private val instances = new ConcurrentHashMap[String, Neo4jConnector]()
  private val shutdownHookRegistered = new java.util.concurrent.atomic.AtomicBoolean(false)

  def shared(config: Neo4jConfig): Neo4jConnector = {
    registerShutdownHook()
    instances.computeIfAbsent(
      config.instanceKey,
      new JFunction[String, Neo4jConnector] {
        override def apply(key: String): Neo4jConnector = {
          val connector = new Neo4jConnector(config)
          connector.verifyConnectivity()
          connector
        }
      }
    )
  }

  /** Creates an unregistered connector the caller is responsible for closing. */
  def dedicated(config: Neo4jConfig): Neo4jConnector = {
    val connector = new Neo4jConnector(config)
    connector.verifyConnectivity()
    connector
  }

  /** Borrows a dedicated connector for the duration of `body`. */
  def using[T](config: Neo4jConfig)(body: Neo4jConnector => T): T = {
    val connector = dedicated(config)
    try body(connector)
    finally connector.close()
  }

  def closeAll(): Unit = {
    instances.values().asScala.foreach { connector =>
      try connector.close()
      catch { case NonFatal(error) => logger.warn(s"Failed to close connector: ${error.getMessage}") }
    }
    instances.clear()
  }

  private def registerShutdownHook(): Unit =
    if (shutdownHookRegistered.compareAndSet(false, true)) {
      Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
        override def run(): Unit = closeAll()
      }, "neo4j-connector-shutdown"))
    }
}
