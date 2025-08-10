// ===========================================================================
// Real-Time Graph Analytics & NYC Taxi Demand Forecasting
// Neo4j schema bootstrap
//
// Graph model
// -----------
//   (:Location  {locationId, zoneName, borough, serviceZone, centroid, ...})
//   (:Borough   {name})
//   (:DemandWindow {locationId, windowStart, windowEnd, tripCount, ...})
//
//   (:Location)-[:IN_BOROUGH]->(:Borough)
//   (:Location)-[:HAS_DEMAND_WINDOW]->(:DemandWindow)
//   (:DemandWindow)-[:NEXT_WINDOW]->(:DemandWindow)
//   (:Location)-[:TRIP_FLOW {windowStart, windowEnd, tripCount, ...}]->(:Location)
//
// Execution
// ---------
//   cypher-shell -a bolt://localhost:7687 -u neo4j -p "$NEO4J_PASSWORD" \
//     --format plain -f conf/neo4j/schema.cypher
//
// The script is idempotent: every statement uses IF NOT EXISTS or replaces an
// existing definition, so it can be re-applied on every deployment.
//
// Requirements
// ------------
//   * Neo4j 5.x. Property existence constraints (Section 2) require the
//     Enterprise edition; comment that section out on Community.
//   * APOC core with apoc.trigger.enabled=true.
//   * Trigger management procedures execute against the system database, so
//     the script switches databases part-way through and switches back.
// ===========================================================================


// ---------------------------------------------------------------------------
// Section 1: Uniqueness constraints
//
// Each constraint creates a backing range index, so no separate index is
// declared for the constrained property combinations below.
// ---------------------------------------------------------------------------

// A taxi zone is identified by its TLC LocationID (1..265).
CREATE CONSTRAINT location_id_unique IF NOT EXISTS
FOR (loc:Location)
REQUIRE loc.locationId IS UNIQUE;

// One demand bucket per (zone, window start). This is the MERGE key used by
// the streaming writer, so a replayed micro-batch converges instead of
// duplicating rows.
CREATE CONSTRAINT demand_window_key_unique IF NOT EXISTS
FOR (win:DemandWindow)
REQUIRE (win.locationId, win.windowStart) IS UNIQUE;

// Boroughs are a small dimension derived from the zone lookup.
CREATE CONSTRAINT borough_name_unique IF NOT EXISTS
FOR (b:Borough)
REQUIRE b.name IS UNIQUE;

// Model registry entries, keyed by the zone and the horizon they forecast.
CREATE CONSTRAINT forecast_model_key_unique IF NOT EXISTS
FOR (m:ForecastModel)
REQUIRE (m.locationId, m.horizonMinutes, m.modelVersion) IS UNIQUE;


// ---------------------------------------------------------------------------
// Section 2: Property existence constraints (Enterprise edition only)
//
// These reject partially-populated nodes at write time rather than letting
// them surface as nulls in the forecasting features.
// ---------------------------------------------------------------------------

CREATE CONSTRAINT location_id_exists IF NOT EXISTS
FOR (loc:Location)
REQUIRE loc.locationId IS NOT NULL;

CREATE CONSTRAINT demand_window_location_exists IF NOT EXISTS
FOR (win:DemandWindow)
REQUIRE win.locationId IS NOT NULL;

CREATE CONSTRAINT demand_window_start_exists IF NOT EXISTS
FOR (win:DemandWindow)
REQUIRE win.windowStart IS NOT NULL;

CREATE CONSTRAINT demand_window_trip_count_exists IF NOT EXISTS
FOR (win:DemandWindow)
REQUIRE win.tripCount IS NOT NULL;

CREATE CONSTRAINT trip_flow_window_start_exists IF NOT EXISTS
FOR ()-[flow:TRIP_FLOW]-()
REQUIRE flow.windowStart IS NOT NULL;


// ---------------------------------------------------------------------------
// Section 3: Node range indexes
//
// Tuned for the three dominant access patterns: time-range scans over demand
// history, top-N demand ranking, and surge filtering.
// ---------------------------------------------------------------------------

// Time-range scans across all zones ("what did the whole city look like
// between two window boundaries").
CREATE INDEX demand_window_start_range IF NOT EXISTS
FOR (win:DemandWindow)
ON (win.windowStart);

CREATE INDEX demand_window_end_range IF NOT EXISTS
FOR (win:DemandWindow)
ON (win.windowEnd);

// Top-N demand ranking within a time slice.
CREATE INDEX demand_window_trip_count IF NOT EXISTS
FOR (win:DemandWindow)
ON (win.tripCount);

CREATE INDEX demand_window_demand_rate IF NOT EXISTS
FOR (win:DemandWindow)
ON (win.demandPerMinute);

// Surge dashboards filter on the flag first, then order by ratio.
CREATE INDEX demand_window_surge IF NOT EXISTS
FOR (win:DemandWindow)
ON (win.isSurge);

// Composite index supporting the per-zone time-series walk used to build
// lag features for the forecaster.
CREATE INDEX demand_window_location_time IF NOT EXISTS
FOR (win:DemandWindow)
ON (win.locationId, win.windowStart);

// Zone dimension lookups.
CREATE INDEX location_borough IF NOT EXISTS
FOR (loc:Location)
ON (loc.borough);

CREATE INDEX location_service_zone IF NOT EXISTS
FOR (loc:Location)
ON (loc.serviceZone);

CREATE INDEX location_latest_window IF NOT EXISTS
FOR (loc:Location)
ON (loc.latestWindowStart);

// Model registry lookups by freshness.
CREATE INDEX forecast_model_horizon IF NOT EXISTS
FOR (m:ForecastModel)
ON (m.horizonMinutes);


// ---------------------------------------------------------------------------
// Section 4: Relationship indexes
//
// TRIP_FLOW edges carry the window key, so traversals must be able to prune
// on it without expanding every edge between a zone pair.
// ---------------------------------------------------------------------------

CREATE INDEX trip_flow_window_start IF NOT EXISTS
FOR ()-[flow:TRIP_FLOW]-()
ON (flow.windowStart);

CREATE INDEX trip_flow_trip_count IF NOT EXISTS
FOR ()-[flow:TRIP_FLOW]-()
ON (flow.tripCount);

CREATE INDEX trip_flow_window_span IF NOT EXISTS
FOR ()-[flow:TRIP_FLOW]-()
ON (flow.windowStart, flow.windowEnd);


// ---------------------------------------------------------------------------
// Section 5: Text, point and full-text indexes
// ---------------------------------------------------------------------------

// Prefix and contains matching on zone names in the dashboard search box.
CREATE TEXT INDEX location_zone_name_text IF NOT EXISTS
FOR (loc:Location)
ON (loc.zoneName);

// Zone centroids, for nearest-neighbour and radius queries against the
// spatial layer.
CREATE POINT INDEX location_centroid_point IF NOT EXISTS
FOR (loc:Location)
ON (loc.centroid);

// Fuzzy search across the zone dimension.
CREATE FULLTEXT INDEX location_search_fulltext IF NOT EXISTS
FOR (loc:Location)
ON EACH [loc.zoneName, loc.borough, loc.serviceZone]
OPTIONS {
  indexConfig: {
    `fulltext.analyzer`: 'standard-folding',
    `fulltext.eventually_consistent`: true
  }
};


// ---------------------------------------------------------------------------
// Section 6: APOC triggers
//
// Trigger administration runs against the system database in Neo4j 5, so the
// session switches here and switches back in Section 7.
//
// Every trigger filters on the label of the node it acts on, which is what
// keeps them from re-entering: a trigger that writes only to :Location can
// fire the :DemandWindow triggers' change events, but their label guards
// discard them immediately.
// ---------------------------------------------------------------------------

:use system;

// --- 6.1 Derive the window end boundary -------------------------------------
// A writer that supplies windowSeconds but omits windowEnd gets the boundary
// filled in, so downstream range queries never see a null upper bound.
CALL apoc.trigger.install(
  'neo4j',
  'demand_window_derive_end',
  "
  UNWIND $createdNodes AS node
  WITH node
  WHERE node:DemandWindow
    AND node.windowEnd IS NULL
    AND node.windowStart IS NOT NULL
    AND node.windowSeconds IS NOT NULL
  SET node.windowEnd = node.windowStart + (node.windowSeconds * 1000)
  ",
  { phase: 'after' }
);

// --- 6.2 Attach demand windows to their zone --------------------------------
// Guarantees the (:Location)-[:HAS_DEMAND_WINDOW]->(:DemandWindow) edge exists
// even when a window arrives before the zone dimension has been seeded.
CALL apoc.trigger.install(
  'neo4j',
  'demand_window_attach_location',
  "
  UNWIND $createdNodes AS node
  WITH node
  WHERE node:DemandWindow AND node.locationId IS NOT NULL
  MERGE (loc:Location {locationId: node.locationId})
  MERGE (loc)-[:HAS_DEMAND_WINDOW]->(node)
  ",
  { phase: 'after' }
);

// --- 6.3 Chain consecutive windows ------------------------------------------
// Builds the :NEXT_WINDOW spine that the forecaster walks to assemble lag
// features. Both directions are linked so that a late-arriving window is
// spliced into the existing chain rather than appended to the end.
CALL apoc.trigger.install(
  'neo4j',
  'demand_window_chain_sequence',
  "
  UNWIND $createdNodes AS node
  WITH node
  WHERE node:DemandWindow
    AND node.locationId IS NOT NULL
    AND node.windowStart IS NOT NULL
  OPTIONAL MATCH (earlier:DemandWindow {locationId: node.locationId})
  WHERE earlier.windowStart < node.windowStart
  WITH node, earlier
  ORDER BY earlier.windowStart DESC
  WITH node, head(collect(earlier)) AS previous
  FOREACH (p IN CASE WHEN previous IS NULL THEN [] ELSE [previous] END |
    MERGE (p)-[:NEXT_WINDOW]->(node)
  )
  WITH node
  OPTIONAL MATCH (later:DemandWindow {locationId: node.locationId})
  WHERE later.windowStart > node.windowStart
  WITH node, later
  ORDER BY later.windowStart ASC
  WITH node, head(collect(later)) AS following
  FOREACH (f IN CASE WHEN following IS NULL THEN [] ELSE [following] END |
    MERGE (node)-[:NEXT_WINDOW]->(f)
  )
  ",
  { phase: 'afterAsync' }
);

// --- 6.4 Roll current demand up onto the zone -------------------------------
// Keeps a denormalised snapshot on :Location so that map rendering does not
// have to traverse into the window history for every tile refresh.
// Keyed on tripCount assignment; the trigger writes only :Location properties,
// so it cannot re-trigger itself.
CALL apoc.trigger.install(
  'neo4j',
  'location_demand_rollup',
  "
  UNWIND coalesce($assignedNodeProperties.tripCount, []) AS assignment
  WITH assignment.node AS window
  WHERE window:DemandWindow
    AND window.locationId IS NOT NULL
    AND window.windowStart IS NOT NULL
  MATCH (loc:Location {locationId: window.locationId})
  SET loc.latestWindowStart =
        CASE WHEN coalesce(loc.latestWindowStart, -1) <= window.windowStart
             THEN window.windowStart ELSE loc.latestWindowStart END,
      loc.latestTripCount =
        CASE WHEN coalesce(loc.latestWindowStart, -1) <= window.windowStart
             THEN window.tripCount ELSE loc.latestTripCount END,
      loc.latestDemandPerMinute =
        CASE WHEN coalesce(loc.latestWindowStart, -1) <= window.windowStart
             THEN window.demandPerMinute ELSE loc.latestDemandPerMinute END,
      loc.peakTripCount =
        CASE WHEN coalesce(loc.peakTripCount, -1) < window.tripCount
             THEN window.tripCount ELSE loc.peakTripCount END,
      loc.observedWindowCount = coalesce(loc.observedWindowCount, 0) + 1
  ",
  { phase: 'afterAsync' }
);

// --- 6.5 Score demand surges ------------------------------------------------
// Compares each window against the trailing mean of the twelve preceding
// windows for the same zone and flags departures above the surge threshold.
// Keyed on demandPerMinute assignment and writes only derived properties, so
// it does not re-enter.
CALL apoc.trigger.install(
  'neo4j',
  'demand_window_surge_score',
  "
  UNWIND coalesce($assignedNodeProperties.demandPerMinute, []) AS assignment
  WITH assignment.node AS window
  WHERE window:DemandWindow
    AND window.demandPerMinute IS NOT NULL
    AND window.locationId IS NOT NULL
  OPTIONAL MATCH (history:DemandWindow {locationId: window.locationId})
  WHERE history.windowStart < window.windowStart
    AND history.demandPerMinute IS NOT NULL
  WITH window, history
  ORDER BY history.windowStart DESC
  WITH window, collect(history.demandPerMinute)[0..12] AS trailing
  WITH window, trailing,
       CASE WHEN size(trailing) = 0 THEN 0.0
            ELSE reduce(total = 0.0, value IN trailing | total + value) / size(trailing)
       END AS baseline
  SET window.baselineDemandPerMinute = baseline,
      window.baselineSampleSize = size(trailing),
      window.surgeRatio =
        CASE WHEN baseline <= 0.0 THEN 0.0
             ELSE window.demandPerMinute / baseline END,
      window.isSurge =
        baseline > 0.0
        AND size(trailing) >= 4
        AND window.demandPerMinute >= baseline * 1.75
  ",
  { phase: 'afterAsync' }
);

// --- 6.6 Maintain the borough dimension -------------------------------------
// Promotes the borough string on a zone into a first-class node so that
// borough-level aggregation is a one-hop traversal.
CALL apoc.trigger.install(
  'neo4j',
  'location_borough_dimension',
  "
  UNWIND coalesce($assignedNodeProperties.borough, []) AS assignment
  WITH assignment.node AS loc
  WHERE loc:Location AND loc.borough IS NOT NULL AND trim(loc.borough) <> ''
  MERGE (b:Borough {name: loc.borough})
  MERGE (loc)-[:IN_BOROUGH]->(b)
  ",
  { phase: 'afterAsync' }
);

// --- 6.7 Normalise flow edge direction metadata -----------------------------
// Marks intra-zone circulation separately from inter-zone movement, which the
// centrality passes exclude, and derives the edge duration span.
CALL apoc.trigger.install(
  'neo4j',
  'trip_flow_classify',
  "
  UNWIND $createdRelationships AS rel
  WITH rel
  WHERE type(rel) = 'TRIP_FLOW'
  WITH rel, startNode(rel) AS origin, endNode(rel) AS destination
  SET rel.isInternal = (origin.locationId = destination.locationId),
      rel.windowSpanSeconds =
        CASE WHEN rel.windowEnd IS NULL OR rel.windowStart IS NULL THEN null
             ELSE (rel.windowEnd - rel.windowStart) / 1000 END
  ",
  { phase: 'after' }
);

// --- 6.8 Guard against out-of-range zone identifiers -------------------------
// The TLC lookup defines zones 1..265. Anything outside that range is a parse
// error upstream; the node is quarantined with a label rather than deleted so
// the ingestion path can be audited.
CALL apoc.trigger.install(
  'neo4j',
  'location_range_quarantine',
  "
  UNWIND $createdNodes AS node
  WITH node
  WHERE node:Location
    AND node.locationId IS NOT NULL
    AND (node.locationId < 1 OR node.locationId > 265)
  SET node:QuarantinedLocation,
      node.quarantineReason = 'location_id_out_of_tlc_range'
  ",
  { phase: 'after' }
);


// ---------------------------------------------------------------------------
// Section 7: Return to the application database and verify
// ---------------------------------------------------------------------------

:use neo4j;

SHOW CONSTRAINTS
YIELD name, type, entityType, labelsOrTypes, properties
RETURN name, type, entityType, labelsOrTypes, properties
ORDER BY name;

SHOW INDEXES
YIELD name, type, entityType, labelsOrTypes, properties, state
RETURN name, type, entityType, labelsOrTypes, properties, state
ORDER BY name;

CALL db.awaitIndexes(300);

:use system;

CALL apoc.trigger.show('neo4j')
YIELD name, installed, paused
RETURN name, installed, paused
ORDER BY name;

:use neo4j;
