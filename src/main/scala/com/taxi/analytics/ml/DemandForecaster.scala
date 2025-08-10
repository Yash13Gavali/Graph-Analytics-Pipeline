package com.taxi.analytics.ml

import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor, RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import com.typesafe.scalalogging.StrictLogging

// ---------------------------------------------------------------------------
// Real-Time Graph Analytics & NYC Taxi Demand Forecasting
// Demand forecasting models
//
// Trains tree ensembles over the matrix produced by FeaturePipeline. Two
// choices here differ from the textbook Spark ML recipe, both for the same
// reason — this is a time series, not an i.i.d. sample:
//
//   Validation  — CrossValidator shuffles rows at random, which places a
//                 window's own neighbours in both folds and reports a score
//                 the model cannot reproduce in production. Validation here
//                 is walk-forward: train on the past, score the next block,
//                 advance, with an embargo between them.
//
//   Baseline    — a forecaster that cannot beat "the same slot yesterday" is
//                 not worth deploying, so every report carries the seasonal
//                 naive comparison and a skill score rather than an isolated
//                 RMSE that looks impressive on its own.
// ---------------------------------------------------------------------------

/** Which ensemble backs the forecaster. */
sealed trait ModelFamily extends Serializable {
  def name: String
}

object ModelFamily {

  /**
   * Bagged trees. Resistant to overfitting, parallelises across trees, and
   * degrades gracefully when a zone's history is short.
   */
  case object RandomForest extends ModelFamily { val name = "random_forest" }

  /**
   * Boosted trees. Usually the stronger fit on this problem, at the cost of
   * sequential training and real sensitivity to depth and step size.
   */
  case object GradientBoosted extends ModelFamily { val name = "gradient_boosted" }

  val All: Seq[ModelFamily] = Seq(RandomForest, GradientBoosted)

  def fromString(raw: String): Either[String, ModelFamily] = {
    val normalized = raw.trim.toLowerCase
    All.find(_.name == normalized).toRight(s"Unknown model family '$raw'; expected ${All.map(_.name).mkString(" or ")}")
  }
}

/** Random forest hyperparameters. */
final case class RandomForestParams(
    numTrees: Int = 120,
    maxDepth: Int = 12,
    maxBins: Int = 64,
    minInstancesPerNode: Int = 5,
    minInfoGain: Double = 0.0,
    subsamplingRate: Double = 0.8,
    featureSubsetStrategy: String = "sqrt",
    maxMemoryInMB: Int = 512,
    cacheNodeIds: Boolean = true
) extends Serializable {

  require(numTrees >= 1, "numTrees must be at least 1")
  require(maxDepth >= 1 && maxDepth <= 30, "maxDepth must be within [1, 30]")
  require(maxBins >= 2, "maxBins must be at least 2")
  require(minInstancesPerNode >= 1, "minInstancesPerNode must be at least 1")
  require(minInfoGain >= 0.0, "minInfoGain must be non-negative")
  require(subsamplingRate > 0.0 && subsamplingRate <= 1.0, "subsamplingRate must be within (0.0, 1.0]")
  require(maxMemoryInMB >= 64, "maxMemoryInMB must be at least 64")

  def describe: String =
    s"trees=$numTrees depth=$maxDepth bins=$maxBins min_instances=$minInstancesPerNode " +
      s"subsample=$subsamplingRate strategy=$featureSubsetStrategy"
}

/** Gradient boosted tree hyperparameters. */
final case class GbtParams(
    maxIter: Int = 150,
    maxDepth: Int = 6,
    stepSize: Double = 0.08,
    maxBins: Int = 64,
    minInstancesPerNode: Int = 5,
    minInfoGain: Double = 0.0,
    subsamplingRate: Double = 0.8,
    featureSubsetStrategy: String = "all",
    lossType: String = "squared",
    validationTol: Double = 0.001,
    maxMemoryInMB: Int = 512,
    cacheNodeIds: Boolean = true
) extends Serializable {

  require(maxIter >= 1, "maxIter must be at least 1")
  require(maxDepth >= 1 && maxDepth <= 30, "maxDepth must be within [1, 30]")
  require(stepSize > 0.0 && stepSize <= 1.0, "stepSize must be within (0.0, 1.0]")
  require(maxBins >= 2, "maxBins must be at least 2")
  require(minInstancesPerNode >= 1, "minInstancesPerNode must be at least 1")
  require(minInfoGain >= 0.0, "minInfoGain must be non-negative")
  require(subsamplingRate > 0.0 && subsamplingRate <= 1.0, "subsamplingRate must be within (0.0, 1.0]")
  require(Set("squared", "absolute").contains(lossType), "lossType must be 'squared' or 'absolute'")
  require(validationTol >= 0.0, "validationTol must be non-negative")

  def describe: String =
    s"iterations=$maxIter depth=$maxDepth step=$stepSize loss=$lossType subsample=$subsamplingRate"
}

/** Everything needed to fit a model. */
final case class TrainingConfig(
    family: ModelFamily = ModelFamily.GradientBoosted,
    randomForest: RandomForestParams = RandomForestParams(),
    gbt: GbtParams = GbtParams(),
    labelColumn: String = FeatureColumns.Target,
    featuresColumn: String = FeatureColumns.FeatureVector,
    predictionColumn: String = "prediction",
    indexCategoricalFeatures: Boolean = true,
    maxCategories: Int = 40,
    embargoMillis: Long = 900000L,
    clampNonNegative: Boolean = true,
    residualQuantileProbabilities: Seq[Double] = Seq(0.05, 0.5, 0.95),
    seed: Long = 42L
) extends Serializable {

  require(maxCategories >= 2, "maxCategories must be at least 2")
  require(embargoMillis >= 0L, "embargoMillis must be non-negative")
  require(
    residualQuantileProbabilities.forall(p => p > 0.0 && p < 1.0),
    "residual quantile probabilities must be within (0.0, 1.0)"
  )
  require(
    residualQuantileProbabilities.distinct.size == residualQuantileProbabilities.size,
    "residual quantile probabilities must be unique"
  )

  def describe: String = family match {
    case ModelFamily.RandomForest => s"family=${family.name} ${randomForest.describe}"
    case ModelFamily.GradientBoosted => s"family=${family.name} ${gbt.describe}"
  }
}

/**
 * Regression metrics computed in a single pass.
 *
 * Poisson deviance is reported alongside the squared-error family because the
 * target is a count: RMSE weights a ten-trip miss in a two-hundred-trip zone
 * the same as in a five-trip zone, and deviance does not.
 */
final case class ForecastMetrics(
    rowCount: Long,
    rmse: Double,
    mae: Double,
    r2: Double,
    smape: Double,
    poissonDeviance: Double,
    meanLabel: Double,
    meanPrediction: Double
) {

  /** Systematic over- or under-forecasting, in trips per window. */
  def bias: Double = meanPrediction - meanLabel

  /** RMSE as a share of the mean level, comparable across zones. */
  def normalisedRmse: Double = if (meanLabel <= 0.0) 0.0 else rmse / meanLabel

  def summary: String =
    f"n=$rowCount%d rmse=$rmse%.4f mae=$mae%.4f r2=$r2%.4f smape=$smape%.2f%% " +
      f"deviance=$poissonDeviance%.4f bias=$bias%+.4f"
}

/** One walk-forward fold. */
final case class FoldResult(
    foldIndex: Int,
    trainRows: Long,
    validationRows: Long,
    trainCutoff: Long,
    validationStart: Long,
    metrics: ForecastMetrics,
    baseline: ForecastMetrics
) {

  /**
   * Fraction of the baseline's error the model removes. Zero means the model
   * matched seasonal naive; negative means it was worse.
   */
  def skillScore: Double =
    if (baseline.rmse <= 0.0) 0.0 else 1.0 - (metrics.rmse / baseline.rmse)

  def summary: String =
    f"fold=$foldIndex%d train=$trainRows%d validate=$validationRows%d " +
      f"rmse=${metrics.rmse}%.4f baseline_rmse=${baseline.rmse}%.4f skill=${skillScore}%+.4f"
}

/** Aggregate outcome of a walk-forward run. */
final case class ValidationReport(folds: Seq[FoldResult]) {

  def meanRmse: Double = if (folds.isEmpty) 0.0 else folds.map(_.metrics.rmse).sum / folds.size
  def meanMae: Double = if (folds.isEmpty) 0.0 else folds.map(_.metrics.mae).sum / folds.size
  def meanSkill: Double = if (folds.isEmpty) 0.0 else folds.map(_.skillScore).sum / folds.size

  /** Spread of fold RMSE; wide spread means the model is regime-sensitive. */
  def rmseStdDev: Double =
    if (folds.size < 2) {
      0.0
    } else {
      val mean = meanRmse
      math.sqrt(folds.map(fold => math.pow(fold.metrics.rmse - mean, 2.0)).sum / (folds.size - 1))
    }

  def beatsBaseline: Boolean = meanSkill > 0.0

  def summary: String =
    f"folds=${folds.size}%d mean_rmse=$meanRmse%.4f stddev=${rmseStdDev}%.4f " +
      f"mean_mae=$meanMae%.4f mean_skill=$meanSkill%+.4f"
}

/** A named feature and its share of total split importance. */
final case class FeatureImportance(name: String, importance: Double, rank: Int)

/** Metadata persisted alongside a saved model. */
final case class ModelMetadata(
    modelVersion: String,
    family: String,
    hyperparameters: String,
    labelColumn: String,
    featuresColumn: String,
    featureNames: Seq[String],
    trainingRows: Long,
    rmse: Double,
    mae: Double,
    r2: Double,
    smape: Double,
    poissonDeviance: Double,
    skillScore: Double,
    residualQuantileProbabilities: Seq[Double],
    residualQuantiles: Seq[Double]
)

/** A fitted model together with what is needed to interpret and apply it. */
final case class TrainedModel(
    pipeline: PipelineModel,
    config: TrainingConfig,
    featureNames: Seq[String],
    trainingRows: Long,
    metrics: ForecastMetrics,
    residualQuantiles: Map[Double, Double]
) {

  def family: ModelFamily = config.family

  /** Residual band for a given probability, empty when not fitted. */
  def residualAt(probability: Double): Double = residualQuantiles.getOrElse(probability, 0.0)

  def summary: String = s"${config.describe} rows=$trainingRows ${metrics.summary}"
}

/** Raised when training or scoring cannot proceed. */
final class ForecastException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

/**
 * Trains, validates and applies demand forecasting models.
 *
 * The forecaster consumes the feature matrix as given; it does not build
 * features. That separation matters because the same matrix has to be
 * reproducible between a training run and a scoring run, and any transform
 * applied here would have to be replayed identically at inference.
 */
final class DemandForecaster(spark: SparkSession, config: TrainingConfig = TrainingConfig()) extends StrictLogging {

  import DemandForecaster._
  import FeatureColumns._
  import spark.implicits._

  // -------------------------------------------------------------------------
  // Training
  // -------------------------------------------------------------------------

  /**
   * Fits the configured ensemble.
   *
   * The feature vector is assembled here only if it is absent, so a caller who
   * already ran `FeaturePipeline.assembleVector` keeps that exact column order
   * and the importances stay interpretable.
   */
  def train(training: DataFrame): TrainedModel = {
    val prepared = ensureFeatureVector(training)
    val rows = prepared.count()

    if (rows == 0L) {
      throw new ForecastException("Training set is empty")
    }

    logger.info(s"Training ${config.describe} on $rows rows")

    val stages = buildStages(prepared)
    val fitted = new Pipeline().setStages(stages.toArray).fit(prepared)

    val names = featureNamesOf(fitted.transform(prepared.limit(1)))
    val scored = applyPipeline(fitted, prepared)
    val metrics = computeMetrics(scored, config.labelColumn, config.predictionColumn)
    val quantiles = residualQuantiles(scored)

    logger.info(s"In-sample fit: ${metrics.summary}")
    logger.warn("In-sample metrics are optimistic by construction; use walkForwardValidate for a deployable estimate")

    TrainedModel(
      pipeline = fitted,
      config = config,
      featureNames = names,
      trainingRows = rows,
      metrics = metrics,
      residualQuantiles = quantiles
    )
  }

  private def buildStages(prepared: DataFrame): Seq[PipelineStage] = {
    val inputColumn =
      if (config.indexCategoricalFeatures) IndexedFeaturesColumn else config.featuresColumn

    val indexer =
      if (!config.indexCategoricalFeatures) {
        Seq.empty[PipelineStage]
      } else {
        // Low-cardinality columns such as community identifiers and weekday
        // are nominal, not ordinal; without this the trees would split them on
        // meaningless numeric thresholds.
        Seq(
          new VectorIndexer()
            .setInputCol(config.featuresColumn)
            .setOutputCol(IndexedFeaturesColumn)
            .setMaxCategories(config.maxCategories)
            .setHandleInvalid("keep")
        )
      }

    val regressor: PipelineStage = config.family match {
      case ModelFamily.RandomForest =>
        val params = config.randomForest
        new RandomForestRegressor()
          .setLabelCol(config.labelColumn)
          .setFeaturesCol(inputColumn)
          .setPredictionCol(RawPredictionColumn)
          .setNumTrees(params.numTrees)
          .setMaxDepth(params.maxDepth)
          .setMaxBins(params.maxBins)
          .setMinInstancesPerNode(params.minInstancesPerNode)
          .setMinInfoGain(params.minInfoGain)
          .setSubsamplingRate(params.subsamplingRate)
          .setFeatureSubsetStrategy(params.featureSubsetStrategy)
          .setMaxMemoryInMB(params.maxMemoryInMB)
          .setCacheNodeIds(params.cacheNodeIds)
          .setSeed(config.seed)

      case ModelFamily.GradientBoosted =>
        val params = config.gbt
        new GBTRegressor()
          .setLabelCol(config.labelColumn)
          .setFeaturesCol(inputColumn)
          .setPredictionCol(RawPredictionColumn)
          .setMaxIter(params.maxIter)
          .setMaxDepth(params.maxDepth)
          .setStepSize(params.stepSize)
          .setMaxBins(params.maxBins)
          .setMinInstancesPerNode(params.minInstancesPerNode)
          .setMinInfoGain(params.minInfoGain)
          .setSubsamplingRate(params.subsamplingRate)
          .setFeatureSubsetStrategy(params.featureSubsetStrategy)
          .setLossType(params.lossType)
          .setValidationTol(params.validationTol)
          .setMaxMemoryInMB(params.maxMemoryInMB)
          .setCacheNodeIds(params.cacheNodeIds)
          .setSeed(config.seed)
    }

    val _ = prepared
    indexer :+ regressor
  }

  /**
   * Applies the pipeline and clamps the output.
   *
   * Tree ensembles interpolate, so a zone with a sparse history can be handed
   * a negative trip count. A negative demand forecast is not a small error, it
   * is a nonsense value that propagates into every downstream allocation.
   */
  private def applyPipeline(model: PipelineModel, frame: DataFrame): DataFrame = {
    val raw = model.transform(frame)
    if (!config.clampNonNegative) {
      raw.withColumnRenamed(RawPredictionColumn, config.predictionColumn)
    } else {
      raw
        .withColumn(config.predictionColumn, greatest(col(RawPredictionColumn), lit(0.0)))
        .drop(RawPredictionColumn)
    }
  }

  private def ensureFeatureVector(frame: DataFrame): DataFrame =
    if (frame.columns.contains(config.featuresColumn)) {
      frame
    } else {
      logger.info("Feature vector column absent; assembling from the numeric columns present")
      val inputs = frame.columns.toSeq
        .filterNot(FeatureColumns.NonFeatureColumns.contains)
        .sorted
        .toArray

      new VectorAssembler()
        .setInputCols(inputs)
        .setOutputCol(config.featuresColumn)
        .setHandleInvalid("error")
        .transform(frame)
    }

  // -------------------------------------------------------------------------
  // Evaluation
  // -------------------------------------------------------------------------

  /** Scores a frame and returns its metrics. */
  def evaluate(model: TrainedModel, frame: DataFrame): ForecastMetrics = {
    val prepared = ensureFeatureVector(frame)
    val scored = applyPipeline(model.pipeline, prepared)
    computeMetrics(scored, config.labelColumn, config.predictionColumn)
  }

  /**
   * Per-zone error breakdown. A model with an acceptable global RMSE can still
   * be unusable in the handful of zones that carry most of the volume, and
   * this is where that shows up.
   */
  def evaluateByZone(model: TrainedModel, frame: DataFrame): DataFrame = {
    val prepared = ensureFeatureVector(frame)
    val scored = applyPipeline(model.pipeline, prepared)

    val error = col(config.predictionColumn) - col(config.labelColumn)

    scored
      .groupBy(col(LocationId))
      .agg(
        count(lit(1)).as("row_count"),
        sqrt(avg(pow(error, 2.0))).as("rmse"),
        avg(abs(error)).as("mae"),
        avg(error).as("bias"),
        avg(col(config.labelColumn)).as("mean_label")
      )
      .withColumn(
        "normalised_rmse",
        when(col("mean_label") > 0.0, col("rmse") / col("mean_label")).otherwise(lit(0.0))
      )
      .orderBy(col("rmse").desc)
  }

  /**
   * Seasonal naive baseline: the trip count observed in the same slot one day
   * earlier. Falls back to a persistence baseline when the daily lag column is
   * absent from the matrix.
   */
  def baselineMetrics(frame: DataFrame): ForecastMetrics = {
    val dailyLag = s"${TripCount}_lag_day"

    val baselineColumn =
      if (frame.columns.contains(dailyLag)) {
        dailyLag
      } else {
        logger.warn(s"Column $dailyLag is absent; falling back to a persistence baseline on $TripCount")
        TripCount
      }

    val scored = frame.withColumn(BaselinePredictionColumn, col(baselineColumn).cast(DoubleType))
    computeMetrics(scored, config.labelColumn, BaselinePredictionColumn)
  }

  /** Fraction of the seasonal naive error the model removes. */
  def skillScore(model: ForecastMetrics, baseline: ForecastMetrics): Double =
    if (baseline.rmse <= 0.0) 0.0 else 1.0 - (model.rmse / baseline.rmse)

  /**
   * Computes every metric in one aggregation pass rather than one scan per
   * metric, which matters on a matrix with millions of rows.
   */
  private def computeMetrics(scored: DataFrame, labelColumn: String, predictionColumn: String): ForecastMetrics = {
    val label = col(labelColumn).cast(DoubleType)
    val prediction = col(predictionColumn).cast(DoubleType)
    val clamped = greatest(prediction, lit(PoissonEpsilon))
    val error = prediction - label

    val smapeTerm: Column = {
      val denominator = abs(label) + abs(prediction)
      when(denominator > 0.0, abs(error) / denominator).otherwise(lit(0.0))
    }

    val devianceTerm: Column =
      lit(2.0) * (
        when(label > 0.0, label * log(label / clamped)).otherwise(lit(0.0)) - (label - clamped)
      )

    val row = scored
      .agg(
        count(lit(1)).as("n"),
        coalesce(sum(label), lit(0.0)).as("sum_label"),
        coalesce(sum(pow(label, 2.0)), lit(0.0)).as("sum_label_sq"),
        coalesce(sum(prediction), lit(0.0)).as("sum_prediction"),
        coalesce(sum(pow(error, 2.0)), lit(0.0)).as("sse"),
        coalesce(sum(abs(error)), lit(0.0)).as("sae"),
        coalesce(sum(smapeTerm), lit(0.0)).as("smape_sum"),
        coalesce(sum(devianceTerm), lit(0.0)).as("deviance_sum")
      )
      .collect()
      .head

    val n = row.getAs[Long]("n")
    if (n == 0L) {
      ForecastMetrics(0L, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    } else {
      val sumLabel = row.getAs[Double]("sum_label")
      val sumLabelSquared = row.getAs[Double]("sum_label_sq")
      val sumPrediction = row.getAs[Double]("sum_prediction")
      val sse = row.getAs[Double]("sse")
      val sae = row.getAs[Double]("sae")
      val smapeSum = row.getAs[Double]("smape_sum")
      val devianceSum = row.getAs[Double]("deviance_sum")

      val count = n.toDouble
      val meanLabel = sumLabel / count
      val totalSumSquares = sumLabelSquared - (sumLabel * sumLabel / count)

      ForecastMetrics(
        rowCount = n,
        rmse = math.sqrt(sse / count),
        mae = sae / count,
        r2 = if (totalSumSquares <= 0.0) 0.0 else 1.0 - (sse / totalSumSquares),
        smape = 200.0 * (smapeSum / count),
        poissonDeviance = devianceSum / count,
        meanLabel = meanLabel,
        meanPrediction = sumPrediction / count
      )
    }
  }

  /**
   * Empirical residual quantiles from the scored frame, used later to attach
   * prediction intervals. Tree ensembles give a point estimate only, so the
   * interval has to come from observed error rather than model variance.
   */
  private def residualQuantiles(scored: DataFrame): Map[Double, Double] = {
    val probabilities = config.residualQuantileProbabilities.toArray
    if (probabilities.isEmpty) {
      Map.empty
    } else {
      val residuals = scored
        .select((col(config.predictionColumn) - col(config.labelColumn)).cast(DoubleType).as("residual"))
        .filter(col("residual").isNotNull)

      val quantiles = residuals.stat.approxQuantile("residual", probabilities, 0.001)
      probabilities.zip(quantiles).toMap
    }
  }

  // -------------------------------------------------------------------------
  // Walk-forward validation
  // -------------------------------------------------------------------------

  /**
   * Expanding-window validation.
   *
   * The matrix is cut into `folds + 1` contiguous blocks by window start. Fold
   * k trains on everything up to block k and scores block k+1, with an embargo
   * of one forecast horizon between them so that no training row's target
   * overlaps the block being scored.
   */
  def walkForwardValidate(frame: DataFrame, folds: Int = 4): ValidationReport = {
    require(folds >= 1, "folds must be at least 1")

    val prepared = ensureFeatureVector(frame).cache()

    try {
      val bounds = prepared.agg(min(col(WindowStart)).as("lo"), max(col(WindowStart)).as("hi")).collect()
      if (bounds.isEmpty || bounds.head.isNullAt(0)) {
        throw new ForecastException("Cannot validate: the matrix has no window bounds")
      }

      val lowerBound = bounds.head.getLong(0)
      val upperBound = bounds.head.getLong(1)
      val span = upperBound - lowerBound

      if (span <= 0L) {
        throw new ForecastException("Cannot validate: the matrix spans a single slot")
      }

      val blockSpan = span / (folds + 1).toLong
      if (blockSpan <= config.embargoMillis) {
        throw new ForecastException(
          s"Fold span ${blockSpan}ms does not exceed the embargo ${config.embargoMillis}ms; reduce folds or widen the range"
        )
      }

      logger.info(s"Walk-forward validation: $folds folds over ${span}ms, block span ${blockSpan}ms")

      val results = (1 to folds).map { foldIndex =>
        val trainCutoff = lowerBound + (blockSpan * foldIndex.toLong)
        val validationStart = trainCutoff + config.embargoMillis
        val validationEnd = lowerBound + (blockSpan * (foldIndex + 1).toLong)

        val trainSlice = prepared.filter(col(WindowStart) < trainCutoff)
        val validationSlice =
          prepared.filter(col(WindowStart) >= validationStart && col(WindowStart) < validationEnd)

        val trainRows = trainSlice.count()
        val validationRows = validationSlice.count()

        if (trainRows == 0L || validationRows == 0L) {
          throw new ForecastException(
            s"Fold $foldIndex is degenerate: train=$trainRows validate=$validationRows"
          )
        }

        val model = train(trainSlice)
        val metrics = evaluate(model, validationSlice)
        val baseline = baselineMetrics(validationSlice)

        val result = FoldResult(
          foldIndex = foldIndex,
          trainRows = trainRows,
          validationRows = validationRows,
          trainCutoff = trainCutoff,
          validationStart = validationStart,
          metrics = metrics,
          baseline = baseline
        )

        logger.info(result.summary)
        result
      }

      val report = ValidationReport(results)
      logger.info(report.summary)

      if (!report.beatsBaseline) {
        logger.warn(
          f"Mean skill ${report.meanSkill}%+.4f: the model does not beat the seasonal naive baseline. " +
            "Check feature health and the lag configuration before deploying."
        )
      }

      report
    } finally {
      val _ = prepared.unpersist(blocking = false)
    }
  }

  /**
   * Grid search scored by walk-forward validation rather than random folds.
   * Returns every candidate ranked by mean fold RMSE, so a caller can weigh
   * the best score against how stable it was across folds.
   */
  def gridSearch(
      frame: DataFrame,
      candidates: Seq[TrainingConfig],
      folds: Int = 3
  ): Seq[(TrainingConfig, ValidationReport)] = {
    require(candidates.nonEmpty, "at least one candidate configuration is required")

    logger.info(s"Grid search over ${candidates.size} candidates with $folds folds each")

    val evaluated = candidates.map { candidate =>
      logger.info(s"Evaluating candidate: ${candidate.describe}")
      val forecaster = new DemandForecaster(spark, candidate)
      val report = forecaster.walkForwardValidate(frame, folds)
      (candidate, report)
    }

    val ranked = evaluated.sortBy { case (_, report) => report.meanRmse }

    ranked.headOption.foreach { case (best, report) =>
      logger.info(s"Best candidate: ${best.describe} | ${report.summary}")
    }

    ranked
  }

  // -------------------------------------------------------------------------
  // Interpretation
  // -------------------------------------------------------------------------

  /**
   * Split-importance per feature, ranked and named.
   *
   * Tree importances are biased toward high-cardinality continuous features,
   * so treat this as a diagnostic for pipeline faults — a feature with zero
   * importance is usually a broken join, not a useless signal — rather than as
   * a causal ranking.
   */
  def featureImportances(model: TrainedModel, topN: Int = 0): Seq[FeatureImportance] = {
    val stages = model.pipeline.stages
    val vector: Vector = stages.last match {
      case forest: RandomForestRegressionModel => forest.featureImportances
      case boosted: GBTRegressionModel => boosted.featureImportances
      case other =>
        throw new ForecastException(s"Final stage ${other.getClass.getSimpleName} exposes no feature importances")
    }

    val values = vector.toArray
    val names =
      if (model.featureNames.size == values.length) model.featureNames
      else values.indices.map(index => s"feature_$index")

    val ranked = names
      .zip(values)
      .sortBy { case (_, importance) => -importance }
      .zipWithIndex
      .map { case ((name, importance), index) => FeatureImportance(name, importance, index + 1) }

    if (topN > 0) ranked.take(topN) else ranked
  }

  /** Logs the leading features and any that contributed nothing. */
  def logFeatureImportances(model: TrainedModel, topN: Int = 20): Unit = {
    val ranked = featureImportances(model)

    val leading = ranked.take(topN).map(entry => f"${entry.name}=${entry.importance}%.4f")
    logger.info(s"Top features: ${leading.mkString(", ")}")

    val unused = ranked.filter(_.importance <= 0.0).map(_.name)
    if (unused.nonEmpty) {
      logger.warn(s"Features with zero importance, usually a broken join: ${unused.mkString(", ")}")
    }
  }

  private def featureNamesOf(transformed: DataFrame): Seq[String] = {
    val column =
      if (config.indexCategoricalFeatures && transformed.columns.contains(IndexedFeaturesColumn)) IndexedFeaturesColumn
      else config.featuresColumn

    val group = AttributeGroup.fromStructField(transformed.schema(column))
    group.attributes
      .map(attributes => attributes.toSeq.zipWithIndex.map { case (attribute, index) =>
        attribute.name.getOrElse(s"feature_$index")
      })
      .getOrElse(Seq.empty)
  }

  // -------------------------------------------------------------------------
  // Inference
  // -------------------------------------------------------------------------

  /**
   * Scores an inference matrix and attaches prediction intervals derived from
   * the model's residual quantiles.
   *
   * The bounds are subtracted, not added: a residual quantile is
   * prediction minus label, so removing the upper residual gives the lower
   * bound of the label.
   */
  def forecast(model: TrainedModel, frame: DataFrame): DataFrame = {
    val prepared = ensureFeatureVector(frame)
    val scored = applyPipeline(model.pipeline, prepared)

    val probabilities = model.residualQuantiles.keys.toSeq.sorted
    val lowerProbability = probabilities.headOption
    val upperProbability = probabilities.lastOption

    val base = scored
      .withColumn(ForecastColumn, col(config.predictionColumn))
      .withColumn(ForecastRoundedColumn, greatest(round(col(ForecastColumn), 0), lit(0.0)).cast(LongType))

    val withInterval = (lowerProbability, upperProbability) match {
      case (Some(low), Some(high)) if low != high =>
        base
          .withColumn(
            ForecastLowerColumn,
            greatest(col(ForecastColumn) - lit(model.residualAt(high)), lit(0.0))
          )
          .withColumn(
            ForecastUpperColumn,
            greatest(col(ForecastColumn) - lit(model.residualAt(low)), lit(0.0))
          )
      case _ =>
        base
          .withColumn(ForecastLowerColumn, col(ForecastColumn))
          .withColumn(ForecastUpperColumn, col(ForecastColumn))
    }

    val selection = Seq(
      Some(col(LocationId)),
      Some(col(WindowStart)),
      if (withInterval.columns.contains(TargetWindowStart)) Some(col(TargetWindowStart)) else None,
      if (withInterval.columns.contains(TripCount)) Some(col(TripCount)) else None,
      Some(col(ForecastColumn)),
      Some(col(ForecastRoundedColumn)),
      Some(col(ForecastLowerColumn)),
      Some(col(ForecastUpperColumn))
    ).flatten

    withInterval.select(selection: _*)
  }

  /** Writes forecasts to Parquet, partitioned by day slot. */
  def writeForecasts(forecasts: DataFrame, path: String, mode: SaveMode = SaveMode.Overwrite): Unit = {
    val partitioned = forecasts.withColumn(PartitionDay, (col(WindowStart) / lit(86400000L)).cast(LongType))

    partitioned.write
      .mode(mode)
      .partitionBy(PartitionDay)
      .option("compression", "snappy")
      .parquet(path)

    logger.info(s"Forecasts written to $path")
  }

  // -------------------------------------------------------------------------
  // Persistence
  // -------------------------------------------------------------------------

  /**
   * Saves the pipeline and a metadata sidecar. The sidecar carries the feature
   * order, which the Spark model format does not preserve in a form the
   * serving path can read back cheaply.
   */
  def save(model: TrainedModel, path: String, modelVersion: String, skillScore: Double = 0.0): Unit = {
    require(modelVersion.trim.nonEmpty, "modelVersion must not be empty")

    model.pipeline.write.overwrite().save(s"$path/$PipelineDirectory")

    val probabilities = model.residualQuantiles.keys.toSeq.sorted

    val metadata = ModelMetadata(
      modelVersion = modelVersion,
      family = model.family.name,
      hyperparameters = model.config.describe,
      labelColumn = model.config.labelColumn,
      featuresColumn = model.config.featuresColumn,
      featureNames = model.featureNames,
      trainingRows = model.trainingRows,
      rmse = model.metrics.rmse,
      mae = model.metrics.mae,
      r2 = model.metrics.r2,
      smape = model.metrics.smape,
      poissonDeviance = model.metrics.poissonDeviance,
      skillScore = skillScore,
      residualQuantileProbabilities = probabilities,
      residualQuantiles = probabilities.map(model.residualAt)
    )

    spark
      .createDataset(Seq(metadata))
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(s"$path/$MetadataDirectory")

    logger.info(s"Model $modelVersion saved to $path")
  }

  /** Reloads a saved pipeline and its metadata. */
  def load(path: String): TrainedModel = {
    val pipeline = PipelineModel.load(s"$path/$PipelineDirectory")
    val metadata = spark.read.json(s"$path/$MetadataDirectory").as[ModelMetadata].collect()

    if (metadata.isEmpty) {
      throw new ForecastException(s"No model metadata found under $path/$MetadataDirectory")
    }

    val stored = metadata.head
    val family = ModelFamily.fromString(stored.family) match {
      case Right(value) => value
      case Left(reason) => throw new ForecastException(reason)
    }

    logger.info(s"Loaded model ${stored.modelVersion} (${stored.family}) with ${stored.featureNames.size} features")

    TrainedModel(
      pipeline = pipeline,
      config = config.copy(family = family, labelColumn = stored.labelColumn, featuresColumn = stored.featuresColumn),
      featureNames = stored.featureNames,
      trainingRows = stored.trainingRows,
      metrics = ForecastMetrics(
        rowCount = stored.trainingRows,
        rmse = stored.rmse,
        mae = stored.mae,
        r2 = stored.r2,
        smape = stored.smape,
        poissonDeviance = stored.poissonDeviance,
        meanLabel = 0.0,
        meanPrediction = 0.0
      ),
      residualQuantiles = stored.residualQuantileProbabilities.zip(stored.residualQuantiles).toMap
    )
  }

  /** Spark's evaluator, exposed for callers that need to plug into ML tuning APIs. */
  def evaluator(metricName: String = "rmse"): RegressionEvaluator =
    new RegressionEvaluator()
      .setLabelCol(config.labelColumn)
      .setPredictionCol(config.predictionColumn)
      .setMetricName(metricName)
}

object DemandForecaster {

  private[ml] val IndexedFeaturesColumn = "indexed_features"
  private[ml] val RawPredictionColumn = "raw_prediction"
  private[ml] val BaselinePredictionColumn = "baseline_prediction"
  private[ml] val PipelineDirectory = "pipeline"
  private[ml] val MetadataDirectory = "metadata"

  /** Floor applied before the logarithm in the Poisson deviance term. */
  private[ml] val PoissonEpsilon = 1e-9

  val ForecastColumn = "forecast_trip_count"
  val ForecastRoundedColumn = "forecast_trip_count_rounded"
  val ForecastLowerColumn = "forecast_lower_bound"
  val ForecastUpperColumn = "forecast_upper_bound"

  def apply(spark: SparkSession, config: TrainingConfig = TrainingConfig()): DemandForecaster =
    new DemandForecaster(spark, config)

  /**
   * Candidate configurations for a grid search. Kept small on purpose: each
   * candidate costs a full walk-forward run, so a broad grid is better
   * explored by narrowing depth and step size first.
   */
  object Grids {

    /** Depth and step size, the two parameters boosted trees are most sensitive to. */
    def gbtDepthAndStep(base: TrainingConfig = TrainingConfig()): Seq[TrainingConfig] =
      for {
        depth <- Seq(4, 6, 8)
        step <- Seq(0.05, 0.1)
      } yield base.copy(
        family = ModelFamily.GradientBoosted,
        gbt = base.gbt.copy(maxDepth = depth, stepSize = step)
      )

    /** Ensemble size against depth for the bagged alternative. */
    def randomForestCapacity(base: TrainingConfig = TrainingConfig()): Seq[TrainingConfig] =
      for {
        trees <- Seq(80, 160)
        depth <- Seq(10, 14)
      } yield base.copy(
        family = ModelFamily.RandomForest,
        randomForest = base.randomForest.copy(numTrees = trees, maxDepth = depth)
      )

    /** One candidate per family, for deciding which to tune further. */
    def familyComparison(base: TrainingConfig = TrainingConfig()): Seq[TrainingConfig] =
      Seq(
        base.copy(family = ModelFamily.RandomForest),
        base.copy(family = ModelFamily.GradientBoosted)
      )
  }
}
