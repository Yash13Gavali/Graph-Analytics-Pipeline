# Real-Time Graph Analytics & NYC Taxi Demand Forecasting

Streaming pipeline that treats NYC taxi demand as a graph problem instead of 265 independent time
series.

Trip events go through Kafka into Spark Structured Streaming, which builds a live demand graph in
Neo4j (zones as nodes, trips between them as weighted edges). GDS routines run over windowed
projections of that graph to get PageRank, betweenness and Louvain communities. Those get joined
with the usual lag/calendar features to train a GBT forecaster. Streamlit app on top for maps and
SHAP.

The idea being tested: a zone's demand depends on its neighbours' demand, and a per-zone model can't
see that. The dashboard reports how much of the model's attribution the graph features actually
account for, so you can check whether the graph layer is pulling its weight.

## Architecture

```
                            +-----------------------------+
                            |   NYC TLC trip record CSV   |
                            +--------------+--------------+
                                           |
                              TaxiTripProducer  (Scala)
                              canonical JSON, deterministic event ids,
                              partition key = pickup zone
                                           |
                                           v
       +-------------------------------------------------------------------+
       |  Apache Kafka          topic: nyc-taxi-trips        6 partitions   |
       +---------------------------------+---------------------------------+
                                         |
                            Spark Structured Streaming
                                  StreamProcessor
                    dedupe within watermark -> sliding window aggregation
                                         |
               +-------------------------+-------------------------+
               |                                                   |
               v                                                   v
      per-zone demand windows                          zone-to-zone flow edges
   (:Location)-[:HAS_DEMAND_WINDOW]->(:DemandWindow)   (:Location)-[:TRIP_FLOW]->(:Location)
               |                                                   |
               +-------------------------+-------------------------+
                                         v
       +-------------------------------------------------------------------+
       |  Neo4j 5   +  APOC triggers  +  Graph Data Science                 |
       |                                                                    |
       |   GraphProjectionBuilder                                           |
       |     windowed, generation-versioned in-memory projections           |
       |            |                                                       |
       |            +--> CentralityAnalytics                                |
       |            |      PageRank      -> sustained demand pull           |
       |            |      Betweenness   -> corridor position               |
       |            |      composite     -> importanceScore, corridorBias   |
       |            |                                                       |
       |            +--> ClusterAnalytics                                   |
       |                   Louvain       -> communityId, cohesion           |
       +---------------------------------+---------------------------------+
                                         |
                             FeaturePipeline  (Spark)
              complete (zone x slot) grid -> lags -> trailing aggregates
              -> cyclical calendar -> graph topology -> flow pressure
                                         |
                                         v
       +-------------------------------------------------------------------+
       |  Feature store        Parquet, partitioned by day slot             |
       +---------------------------------+---------------------------------+
                                         |
                          DemandForecaster  (Spark MLlib)
                  Random Forest / GBT, walk-forward validated against
                  a seasonal naive baseline, residual-quantile intervals
                                         |
                     +-------------------+-------------------+
                     |                                       |
                     v                                       v
         forecasts (Parquet)                   (:DemandForecast) nodes in Neo4j
                     |                                       |
                     +-------------------+-------------------+
                                         v
       +-------------------------------------------------------------------+
       |  Streamlit dashboard                                               |
       |    Kepler.gl   zone demand, flow arcs, forecast delta              |
       |    Plotly      prediction intervals, centrality scatter            |
       |    SHAP        surrogate attribution over deployed predictions     |
       +-------------------------------------------------------------------+


  PipelineDriver runs three loops:
     continuous  ->  streaming ingestion
     periodic    ->  projection refresh + centrality + communities
     periodic    ->  feature assembly + scoring
```

## Layout

```
.
├── build.sbt
├── project/plugins.sbt
├── requirements.txt
├── docker-compose.yml
│
├── conf/
│   ├── neo4j/schema.cypher            constraints, indexes, APOC triggers
│   └── spark/                         mounted Spark overrides
│
├── src/
│   ├── main/scala/com/taxi/analytics/
│   │   ├── PipelineDriver.scala       entry point, five run modes
│   │   ├── ingestion/TaxiTripProducer.scala
│   │   ├── processor/StreamProcessor.scala
│   │   ├── database/Neo4jConnector.scala
│   │   ├── graph/
│   │   │   ├── GraphProjectionBuilder.scala
│   │   │   ├── CentralityAnalytics.scala
│   │   │   └── ClusterAnalytics.scala
│   │   └── ml/
│   │       ├── FeaturePipeline.scala
│   │       └── DemandForecaster.scala
│   └── test/scala/com/taxi/analytics/PipelineSpec.scala
│
├── dashboard/app.py
├── data/                              CSVs, zone geometry, feature store, models
├── jars/                              assembled jars for spark-submit
└── checkpoints/                       streaming checkpoint root
```

## Requirements

Docker Engine with Compose v2, JDK 8 or 11, sbt 1.9.x, Python 3.10–3.11. Scala 2.12.18 is managed by
sbt.

Neo4j runs on the Enterprise image because GDS needs it for the full algorithm set. Community works
if you drop `graph-data-science` from `NEO4J_PLUGINS`, but then no centrality or community passes.

## Setup

### 1. Environment

`.env` in the repo root (gitignored):

```bash
NEO4J_PASSWORD=choose_a_local_password
NEO4J_HEAP_MAX=2G
NEO4J_PAGECACHE=1G
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2G
```

Export it too, for anything run outside Compose:

```bash
export NEO4J_PASSWORD=choose_a_local_password
```

Nothing takes the password as a CLI argument, so it won't show up in a process listing or the Spark
UI.

### 2. Directories

Compose bind-mounts these. Create them first or Docker makes them root-owned:

```bash
mkdir -p data/neo4j-import jars checkpoints conf/spark conf/neo4j
```

### 3. Start

```bash
docker compose up -d
docker compose ps
```

Wait for healthy. Endpoints:

- Kafka from host `localhost:29092`, in-network `kafka:9092`
- Neo4j Browser http://localhost:7474, Bolt `bolt://localhost:7687`
- Spark master UI http://localhost:8080, workers on 8081 and 8082

### 4. Schema

Run before the first write. Without the constraints, concurrent `MERGE` creates duplicates instead of
failing:

```bash
docker compose exec neo4j cypher-shell \
  -u neo4j -p "$NEO4J_PASSWORD" \
  --format plain \
  -f /var/lib/neo4j/conf/custom/schema.cypher
```

Idempotent, so re-running on deploy is fine. It switches to the `system` database for the APOC
trigger section, which is why it needs `cypher-shell` and won't work pasted into Browser.

Check with `SHOW CONSTRAINTS;` and `CALL apoc.trigger.show('neo4j');`.

### 5. Topic

Auto-create is off so topics get the right partition count:

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic nyc-taxi-trips \
  --partitions 6 --replication-factor 1
```

### 6. Build

```bash
sbt clean compile
sbt test
sbt assembly
```

`sbt test` covers the unit and Spark tiers. The Neo4j integration tests are skipped unless you give
them somewhere to connect:

```bash
export NEO4J_TEST_URI=bolt://localhost:7687
export NEO4J_TEST_PASSWORD="$NEO4J_PASSWORD"
sbt test
```

Coverage: `sbt clean coverage test coverageReport`.

## Running it

### Replay trips into Kafka

```bash
java -cp jars/taxi-graph-ingestion.jar \
  com.taxi.analytics.ingestion.TaxiTripProducer \
  --input data/yellow_tripdata.csv \
  --bootstrap-servers localhost:29092 \
  --service-type yellow \
  --rate 500
```

`--rate` is records/sec, `0` for unthrottled. Event ids come from each trip's natural key, so
replaying the same file twice doesn't duplicate anything downstream.

### Driver

Five modes, all reading `NEO4J_PASSWORD` from the environment:

| Mode | Does |
| --- | --- |
| `stream` | ingestion only, blocks until a query stops |
| `graph` | one projection refresh + centrality + communities |
| `train` | one feature build, walk-forward validation, model fit |
| `infer` | one feature build and scoring pass |
| `all` | streaming plus scheduled graph and inference cycles |

```bash
spark-submit \
  --master spark://localhost:7077 \
  --class com.taxi.analytics.PipelineDriver \
  --conf spark.executorEnv.NEO4J_PASSWORD="$NEO4J_PASSWORD" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  jars/nyc-taxi-graph-analytics.jar \
  --mode all \
  --bootstrap-servers kafka:9092 \
  --window-duration "15 minutes" \
  --slide-duration "5 minutes" \
  --horizon-slots 3 \
  --graph-refresh-minutes 15 \
  --inference-minutes 5 \
  --publish-forecasts-to-graph
```

First run has no model to score with, so go in order: `--mode stream` until there are enough windows
for a projection, then `--mode graph`, then `--mode train --model-version v1`, then `--mode all`.

`--help` on any entry point for the full option list.

### Dashboard

```bash
pip install -r requirements.txt

export NEO4J_URI=bolt://localhost:7687
export NEO4J_PASSWORD="$NEO4J_PASSWORD"
export FEATURE_STORE_PATH=data/features
export FORECAST_PATH=data/forecasts
export TAXI_ZONE_GEOJSON=data/taxi_zones.geojson

streamlit run dashboard/app.py
```

Falls back to centroid points if the zone polygons aren't there. The explainability tab tells you
what's missing rather than erroring when the feature store is empty.

Zone centroids need to be on the `:Location` nodes as Neo4j points for maps to render. They come from
the TLC zone lookup, not from the trip stream.

## Evaluation

### Walk-forward validation

k-fold shuffles rows, which puts a window's own neighbours on both sides of the split. The score that
comes out isn't reproducible in production.

`walkForwardValidate` cuts the matrix into contiguous blocks by window start. Fold *k* trains on
everything up to block *k*, scores block *k+1*, then the training range expands. There's an embargo
of one forecast horizon between the training cutoff and the validation start, because a training row
near the cutoff has its target inside the validation block.

Each fold also scores a seasonal naive baseline (same slot, one day earlier). `skillScore` is the
fraction of the baseline's error the model removes. RMSE on its own doesn't tell you much. If the
model can't beat "same slot yesterday" it isn't worth deploying, and the report says so.

Spread of fold RMSE matters too. Wide spread means the model works in some regimes and not others,
which the mean hides.

### Poisson deviance

Trip counts are counts. RMSE penalises a 10-trip miss the same in a 200-trip zone as in a 5-trip one,
which makes any model look good if it nails the busy zones and gives up on the quiet ones.

Poisson deviance scales with the expected level:

```
D = 2 * mean( y * log(y / y_hat) - (y - y_hat) )
```

with the `y * log` term zero where `y = 0`. Both metrics get reported, plus a per-zone breakdown,
because a fine global RMSE routinely hides bad behaviour in the few zones carrying most of the volume.

Predictions are clamped at zero. Tree ensembles interpolate and will happily return a negative trip
count for a sparse zone.

### SHAP surrogate

The deployed model is a Spark MLlib ensemble and can't be loaded into the Python process running the
dashboard. Training a separate Python model and calling its attributions the deployed model's would
be wrong, and skipping explainability isn't great either.

So: fit a LightGBM surrogate against the deployed model's own predictions (joined from the forecast
table on `location_id` + `window_start`) and run SHAP over that. The attributions explain the model's
behaviour, not taxi demand in general.

Surrogate fidelity shows as R² against held-out deployed predictions. Below 0.85 the UI says the
attributions are directional only. An explanation is worth as much as the surrogate's agreement with
what it claims to explain, so that number sits next to the chart.

Two derived readings on that tab:

- **Graph attribution share** — how much of total `|SHAP|` comes from centrality, community and flow
  features. If the graph layer isn't earning its cost, this is where you find out.
- **Zero-importance features** — on this pipeline that's nearly always a broken join upstream, not a
  useless signal.

### Graph feature leakage

Worth being explicit about. Centrality scores and community ids are stored as *current* values on the
zone nodes. Join them onto historical rows and you leak: the PageRank a zone has now was computed
from trips that hadn't happened when that row was labelled. Fine at inference, bad for backtesting.

`FeaturePipeline` makes you pick:

- `CurrentNodeProperties` — right for inference, logs a warning during training
- `Snapshots` — as-of join against per-refresh graph snapshots, the leakage-free path, needs
  `--use-graph-snapshots` to have been running long enough to build history
- `Disabled` — demand-only ablation

Run the ablation. If the graph features don't move the skill score, the graph layer is decoration.

## Configuration

Neo4j access, used by every component:

| Variable | Default |
| --- | --- |
| `NEO4J_URI` | `bolt://neo4j:7687` |
| `NEO4J_USER` | `neo4j` |
| `NEO4J_PASSWORD` | required, env only |
| `NEO4J_DATABASE` | `neo4j` |
| `NEO4J_MAX_POOL_SIZE` | `32` |
| `NEO4J_WRITE_BATCH_SIZE` | `2000` |
| `NEO4J_TRACK_BOOKMARKS` | `true` |

Dashboard:

| Variable | Default |
| --- | --- |
| `FEATURE_STORE_PATH` | `/opt/spark/data/features` |
| `FORECAST_PATH` | `/opt/spark/data/forecasts` |
| `TAXI_ZONE_GEOJSON` | `/opt/spark/data/taxi_zones.geojson` |
| `DISPLAY_TIMEZONE` | `America/New_York` |
| `DASHBOARD_CACHE_TTL_SECONDS` | `60` |
| `SURROGATE_SAMPLE_ROWS` | `40000` |

Tests: `NEO4J_TEST_URI` and `NEO4J_TEST_PASSWORD` turn on the integration tier, `NEO4J_TEST_USER`
defaults to `neo4j`.

## Notes

**Kafka listeners.** Two of them. Containers use `kafka:9092`, the host uses `localhost:29092`. A
single-listener setup breaks as soon as a Spark executor tries to reach the broker.

**Projection generations.** GDS has no rename and no atomic replace, so dropping before rebuilding
leaves a gap where readers get "graph does not exist". Generation *n+1* gets built before *n* is
retired. `retainGenerations` controls how many stay resident, and each one costs heap.

**Community id churn.** Louvain renumbers communities between runs even when the partition is
identical, so comparing raw ids is pointless. `ClusterAnalytics.stability` uses a Rand index, which
counts zone pairs both partitions agree on and is invariant to relabelling. Use a seeded run if you
need stable ids.

**Louvain needs undirected input.** Build the projection with `Orientation.Undirected`. The raw GDS
error doesn't say that, so `ClusterAnalytics` rewrites it.

**Grid completeness.** A zone with no trips writes no row, and lag features are defined in slots, not
rows. `FeaturePipeline` builds the full `(zone x slot)` grid before any window function runs. Counts
fill with zero; rates like speed carry forward, since no trips isn't zero mph.

**Cycle failures.** One failed graph or inference cycle gets logged and skipped, usually a transient
broker or DB blip. Five in a row stops the driver.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `Projection has no relationships` | lookback too narrow or `minTripCount` too high |
| Louvain modularity under 0.3 | partition is near-arbitrary, widen the window or lower the threshold |
| All centrality scores identical | projection is empty or edgeless, check `sourceCardinality` |
| Maps render blank | `Location.centroid` not populated from the zone lookup |
| Explainability tab empty | no feature store, run `--mode train` |
| Negative skill score | model loses to seasonal naive, check feature health and lags |
| `graph does not exist` mid-cycle | projection dropped with no retained generation |
