"""Real-Time Graph Analytics & NYC Taxi Demand Forecasting — operator dashboard.

Reads the demand graph directly from Neo4j for live state, and the feature
store and forecast tables from Parquet for anything that needs the full
feature matrix.

Explainability note: the deployed forecaster is a Spark MLlib ensemble and
cannot be loaded into this process. The SHAP view therefore fits a gradient
boosted surrogate against the *deployed model's own predictions* rather than
against the ground truth, which makes it an explanation of the model's
behaviour rather than of the underlying phenomenon. The surrogate's fidelity
to the deployed model is reported alongside every explanation, and a low
fidelity score means the attributions should not be trusted.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from keplergl import KeplerGl
from neo4j import GraphDatabase, basic_auth
from streamlit_keplergl import keplergl_static

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

NYC_CENTER_LATITUDE = 40.7128
NYC_CENTER_LONGITUDE = -74.0060
MILLIS_PER_HOUR = 3_600_000
MILLIS_PER_MINUTE = 60_000

NON_FEATURE_COLUMNS = {
    "location_id",
    "window_start",
    "window_end",
    "target_trip_count",
    "target_window_start",
    "window_day",
    "features",
    "scaled_features",
    "forecast_trip_count",
    "forecast_trip_count_rounded",
    "forecast_lower_bound",
    "forecast_upper_bound",
}


@dataclass(frozen=True)
class Settings:
    """Runtime configuration, sourced entirely from the environment."""

    neo4j_uri: str
    neo4j_user: str
    neo4j_password: str
    neo4j_database: str
    feature_store_path: str
    forecast_path: str
    zone_geojson_path: str
    display_timezone: str
    cache_ttl_seconds: int
    surrogate_sample_rows: int

    @property
    def has_password(self) -> bool:
        return bool(self.neo4j_password)


def load_settings() -> Settings:
    """Builds settings from environment variables with local-friendly defaults."""
    return Settings(
        neo4j_uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_user=os.getenv("NEO4J_USER", "neo4j"),
        neo4j_password=os.getenv("NEO4J_PASSWORD", ""),
        neo4j_database=os.getenv("NEO4J_DATABASE", "neo4j"),
        feature_store_path=os.getenv("FEATURE_STORE_PATH", "/opt/spark/data/features"),
        forecast_path=os.getenv("FORECAST_PATH", "/opt/spark/data/forecasts"),
        zone_geojson_path=os.getenv("TAXI_ZONE_GEOJSON", "/opt/spark/data/taxi_zones.geojson"),
        display_timezone=os.getenv("DISPLAY_TIMEZONE", "America/New_York"),
        cache_ttl_seconds=int(os.getenv("DASHBOARD_CACHE_TTL_SECONDS", "60")),
        surrogate_sample_rows=int(os.getenv("SURROGATE_SAMPLE_ROWS", "40000")),
    )


# ---------------------------------------------------------------------------
# Neo4j access
# ---------------------------------------------------------------------------


@st.cache_resource(show_spinner=False)
def get_driver(uri: str, user: str, password: str):
    """One driver per process; the connection pool lives for the app's lifetime."""
    driver = GraphDatabase.driver(
        uri,
        auth=basic_auth(user, password),
        max_connection_pool_size=16,
        connection_acquisition_timeout=30,
    )
    driver.verify_connectivity()
    return driver


def run_query(
    _driver, database: str, cypher: str, parameters: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """Executes a read query and returns the records as a DataFrame."""
    with _driver.session(database=database) as session:
        result = session.run(cypher, parameters or {})
        records = [record.data() for record in result]
    return pd.DataFrame(records)


LATEST_WINDOW_QUERY = """
MATCH (window:DemandWindow)
RETURN max(window.windowStart) AS latest_window_start,
       min(window.windowStart) AS earliest_window_start,
       count(window) AS window_count
"""

ZONES_QUERY = """
MATCH (zone:Location)
RETURN zone.locationId              AS location_id,
       coalesce(zone.zoneName, '')   AS zone_name,
       coalesce(zone.borough, '')    AS borough,
       coalesce(zone.serviceZone, '') AS service_zone,
       zone.latestWindowStart        AS latest_window_start,
       coalesce(zone.latestTripCount, 0)        AS latest_trip_count,
       coalesce(zone.latestDemandPerMinute, 0.0) AS latest_demand_per_minute,
       coalesce(zone.peakTripCount, 0)          AS peak_trip_count,
       coalesce(zone.pageRankScore, 0.0)        AS page_rank,
       coalesce(zone.betweennessScore, 0.0)     AS betweenness,
       coalesce(zone.importanceScore, 0.0)      AS importance,
       coalesce(zone.importanceRank, 0)         AS importance_rank,
       coalesce(zone.corridorBias, 0.0)         AS corridor_bias,
       zone.communityId              AS community_id,
       zone.centroid.latitude        AS latitude,
       zone.centroid.longitude       AS longitude
ORDER BY location_id
"""

DEMAND_WINDOWS_QUERY = """
MATCH (window:DemandWindow)
WHERE window.windowStart >= $since
RETURN window.locationId                        AS location_id,
       window.windowStart                       AS window_start,
       window.windowEnd                         AS window_end,
       coalesce(window.tripCount, 0)            AS trip_count,
       coalesce(window.passengerTotal, 0)       AS passenger_total,
       coalesce(window.demandPerMinute, 0.0)    AS demand_per_minute,
       coalesce(window.totalRevenue, 0.0)       AS total_revenue,
       coalesce(window.avgSpeedMph, 0.0)        AS avg_speed_mph,
       coalesce(window.avgTripDistanceMiles, 0.0) AS avg_trip_distance_miles,
       coalesce(window.avgDurationSeconds, 0.0) AS avg_duration_seconds,
       coalesce(window.surgeRatio, 0.0)         AS surge_ratio,
       coalesce(window.isSurge, false)          AS is_surge
ORDER BY window_start
"""

ZONE_HISTORY_QUERY = """
MATCH (window:DemandWindow {locationId: $location_id})
WHERE window.windowStart >= $since
RETURN window.windowStart                    AS window_start,
       coalesce(window.tripCount, 0)         AS trip_count,
       coalesce(window.demandPerMinute, 0.0) AS demand_per_minute,
       coalesce(window.baselineDemandPerMinute, 0.0) AS baseline_demand_per_minute,
       coalesce(window.surgeRatio, 0.0)      AS surge_ratio,
       coalesce(window.isSurge, false)       AS is_surge
ORDER BY window_start
"""

FLOWS_QUERY = """
MATCH (origin:Location)-[flow:TRIP_FLOW]->(destination:Location)
WHERE flow.windowStart >= $since
  AND coalesce(flow.tripCount, 0) >= $min_trips
  AND origin.locationId <> destination.locationId
WITH origin, destination,
     sum(flow.tripCount) AS trips,
     avg(flow.avgDurationSeconds) AS avg_duration_seconds,
     avg(flow.avgTripDistanceMiles) AS avg_distance_miles
RETURN origin.locationId            AS origin_id,
       coalesce(origin.zoneName, '') AS origin_name,
       coalesce(origin.borough, '')  AS origin_borough,
       origin.centroid.latitude     AS origin_lat,
       origin.centroid.longitude    AS origin_lng,
       destination.locationId       AS destination_id,
       coalesce(destination.zoneName, '') AS destination_name,
       coalesce(destination.borough, '')  AS destination_borough,
       destination.centroid.latitude  AS destination_lat,
       destination.centroid.longitude AS destination_lng,
       trips,
       coalesce(avg_duration_seconds, 0.0) AS avg_duration_seconds,
       coalesce(avg_distance_miles, 0.0)   AS avg_distance_miles
ORDER BY trips DESC
LIMIT $limit
"""

COMMUNITIES_QUERY = """
MATCH (community:ZoneCommunity)
RETURN community.communityId                      AS community_id,
       coalesce(community.size, 0)                AS size,
       coalesce(community.cohesion, 0.0)          AS cohesion,
       coalesce(community.dominantBorough, '')    AS dominant_borough,
       coalesce(community.dominantBoroughShare, 0.0) AS dominant_borough_share,
       coalesce(community.boroughSpread, 0)       AS borough_spread,
       coalesce(community.internalTripCount, 0)   AS internal_trip_count,
       coalesce(community.externalTripCount, 0)   AS external_trip_count
ORDER BY size DESC
"""

FORECASTS_QUERY = """
MATCH (zone:Location)-[:HAS_FORECAST]->(forecast:DemandForecast)
WHERE forecast.targetWindowStart >= $since
RETURN forecast.locationId                    AS location_id,
       coalesce(zone.zoneName, '')            AS zone_name,
       coalesce(zone.borough, '')             AS borough,
       forecast.originWindowStart             AS origin_window_start,
       forecast.targetWindowStart             AS target_window_start,
       coalesce(forecast.forecastTripCount, 0.0) AS forecast_trip_count,
       coalesce(forecast.lowerBound, 0.0)     AS lower_bound,
       coalesce(forecast.upperBound, 0.0)     AS upper_bound,
       coalesce(forecast.modelVersion, '')    AS model_version,
       coalesce(forecast.horizonSlots, 0)     AS horizon_slots,
       zone.centroid.latitude                 AS latitude,
       zone.centroid.longitude                AS longitude
ORDER BY target_window_start, location_id
"""


# ---------------------------------------------------------------------------
# Cached loaders
# ---------------------------------------------------------------------------


@st.cache_data(ttl=60, show_spinner=False)
def load_window_bounds(uri: str, user: str, password: str, database: str) -> Dict[str, Any]:
    driver = get_driver(uri, user, password)
    frame = run_query(driver, database, LATEST_WINDOW_QUERY)
    if frame.empty or pd.isna(frame.iloc[0]["latest_window_start"]):
        return {"latest": None, "earliest": None, "count": 0}
    row = frame.iloc[0]
    return {
        "latest": int(row["latest_window_start"]),
        "earliest": int(row["earliest_window_start"]),
        "count": int(row["window_count"]),
    }


@st.cache_data(ttl=60, show_spinner=False)
def load_zones(uri: str, user: str, password: str, database: str) -> pd.DataFrame:
    driver = get_driver(uri, user, password)
    frame = run_query(driver, database, ZONES_QUERY)
    if frame.empty:
        return frame
    frame["community_id"] = frame["community_id"].fillna(-1).astype(int)
    return frame


@st.cache_data(ttl=60, show_spinner=False)
def load_demand_windows(
    uri: str, user: str, password: str, database: str, since: int
) -> pd.DataFrame:
    driver = get_driver(uri, user, password)
    return run_query(driver, database, DEMAND_WINDOWS_QUERY, {"since": since})


@st.cache_data(ttl=60, show_spinner=False)
def load_zone_history(
    uri: str, user: str, password: str, database: str, location_id: int, since: int
) -> pd.DataFrame:
    driver = get_driver(uri, user, password)
    return run_query(
        driver, database, ZONE_HISTORY_QUERY, {"location_id": location_id, "since": since}
    )


@st.cache_data(ttl=60, show_spinner=False)
def load_flows(
    uri: str, user: str, password: str, database: str, since: int, min_trips: int, limit: int
) -> pd.DataFrame:
    driver = get_driver(uri, user, password)
    return run_query(
        driver, database, FLOWS_QUERY, {"since": since, "min_trips": min_trips, "limit": limit}
    )


@st.cache_data(ttl=60, show_spinner=False)
def load_communities(uri: str, user: str, password: str, database: str) -> pd.DataFrame:
    driver = get_driver(uri, user, password)
    return run_query(driver, database, COMMUNITIES_QUERY)


@st.cache_data(ttl=60, show_spinner=False)
def load_forecasts_from_graph(
    uri: str, user: str, password: str, database: str, since: int
) -> pd.DataFrame:
    driver = get_driver(uri, user, password)
    return run_query(driver, database, FORECASTS_QUERY, {"since": since})


@st.cache_data(ttl=300, show_spinner=False)
def load_parquet(path: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """Reads a Parquet dataset, returning an empty frame when it is absent."""
    location = Path(path)
    if not location.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(location, columns=columns)
    except (OSError, ValueError) as error:
        st.warning(f"Could not read {path}: {error}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def load_zone_geojson(path: str) -> Optional[Dict[str, Any]]:
    """Loads the TLC taxi zone polygons if they have been provisioned."""
    location = Path(path)
    if not location.exists():
        return None
    try:
        with location.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        st.warning(f"Could not read zone polygons: {error}")
        return None


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def to_local_datetime(series: pd.Series, timezone: str) -> pd.Series:
    """Converts epoch milliseconds to a timezone-aware local series."""
    return pd.to_datetime(series, unit="ms", utc=True).dt.tz_convert(timezone)


def format_window(epoch_millis: Optional[int], timezone: str) -> str:
    if epoch_millis is None:
        return "unavailable"
    stamp = pd.to_datetime(epoch_millis, unit="ms", utc=True).tz_convert(timezone)
    return stamp.strftime("%H:%M %Z")


def safe_ratio(numerator: float, denominator: float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


# ---------------------------------------------------------------------------
# Kepler.gl map construction
# ---------------------------------------------------------------------------


def kepler_config(zoom: float = 9.5) -> Dict[str, Any]:
    """Map state only; layers are auto-detected from the supplied datasets.

    Pinning explicit layer definitions here breaks whenever a column is added,
    and kepler's own detection handles the point and arc cases correctly.
    """
    return {
        "version": "v1",
        "config": {
            "mapState": {
                "latitude": NYC_CENTER_LATITUDE,
                "longitude": NYC_CENTER_LONGITUDE,
                "zoom": zoom,
                "bearing": 0,
                "pitch": 0,
            },
            "mapStyle": {"styleType": "dark"},
        },
    }


def enrich_geojson(
    geojson: Dict[str, Any], metrics: pd.DataFrame, value_columns: List[str]
) -> Dict[str, Any]:
    """Copies per-zone metrics into the polygon properties for choropleth use.

    The TLC file has been published with both ``location_id`` and ``LocationID``
    spellings, so both are accepted.
    """
    lookup: Dict[int, Dict[str, Any]] = {}
    for record in metrics.to_dict("records"):
        try:
            lookup[int(record["location_id"])] = record
        except (KeyError, TypeError, ValueError):
            continue

    enriched = {"type": geojson.get("type", "FeatureCollection"), "features": []}

    for feature in geojson.get("features", []):
        properties = dict(feature.get("properties", {}))
        raw_id = properties.get("location_id", properties.get("LocationID"))
        try:
            zone_id = int(raw_id)
        except (TypeError, ValueError):
            zone_id = None

        metrics_row = lookup.get(zone_id, {}) if zone_id is not None else {}
        for column in value_columns:
            properties[column] = metrics_row.get(column, 0)
        properties["location_id"] = zone_id if zone_id is not None else -1
        properties["zone_name"] = metrics_row.get("zone_name", properties.get("zone", ""))

        enriched["features"].append(
            {
                "type": feature.get("type", "Feature"),
                "geometry": feature.get("geometry"),
                "properties": properties,
            }
        )

    return enriched


def render_kepler(datasets: Dict[str, Any], height: int = 620, zoom: float = 9.5) -> None:
    """Renders a Kepler map, skipping datasets that came back empty."""
    usable = {
        name: data
        for name, data in datasets.items()
        if data is not None and (not isinstance(data, pd.DataFrame) or not data.empty)
    }

    if not usable:
        st.info("No spatial data available for the selected range.")
        return

    kepler_map = KeplerGl(height=height, data=usable, config=kepler_config(zoom))
    keplergl_static(kepler_map, height=height)


# ---------------------------------------------------------------------------
# Surrogate explainability
# ---------------------------------------------------------------------------


@dataclass
class SurrogateExplanation:
    """A fitted surrogate together with the evidence for trusting it."""

    feature_names: List[str]
    shap_values: np.ndarray
    feature_frame: pd.DataFrame
    base_value: float
    fidelity_r2: float
    explained_target: str
    row_count: int

    @property
    def is_faithful(self) -> bool:
        """Below this the surrogate does not reproduce the deployed model."""
        return self.fidelity_r2 >= 0.85

    def global_importance(self) -> pd.DataFrame:
        magnitude = np.abs(self.shap_values).mean(axis=0)
        frame = pd.DataFrame(
            {"feature": self.feature_names, "mean_abs_shap": magnitude}
        ).sort_values("mean_abs_shap", ascending=False)
        return frame.reset_index(drop=True)

    def local_contributions(self, row_index: int, top_n: int = 12) -> pd.DataFrame:
        values = self.shap_values[row_index]
        frame = pd.DataFrame(
            {
                "feature": self.feature_names,
                "shap_value": values,
                "feature_value": self.feature_frame.iloc[row_index].values,
            }
        )
        frame["magnitude"] = frame["shap_value"].abs()
        return frame.sort_values("magnitude", ascending=False).head(top_n).reset_index(drop=True)


@st.cache_data(ttl=900, show_spinner=True)
def fit_surrogate(
    feature_store_path: str, forecast_path: str, sample_rows: int
) -> Optional[SurrogateExplanation]:
    """Fits a tree surrogate against the deployed model's predictions.

    Falls back to the observed target when no forecast table is present, which
    explains the phenomenon rather than the model; the returned
    ``explained_target`` records which of the two happened.
    """
    import lightgbm as lgb
    import shap

    features = load_parquet(feature_store_path)
    if features.empty:
        return None

    forecasts = load_parquet(forecast_path)

    if not forecasts.empty and {"location_id", "window_start", "forecast_trip_count"}.issubset(
        forecasts.columns
    ):
        merged = features.merge(
            forecasts[["location_id", "window_start", "forecast_trip_count"]],
            on=["location_id", "window_start"],
            how="inner",
        )
        target_column = "forecast_trip_count"
        explained_target = "deployed model predictions"
    else:
        merged = features
        target_column = "target_trip_count"
        explained_target = "observed demand"

    if merged.empty or target_column not in merged.columns:
        return None

    merged = merged.dropna(subset=[target_column])
    if len(merged) > sample_rows:
        merged = merged.sample(n=sample_rows, random_state=42)

    feature_columns = [
        column
        for column in merged.columns
        if column not in NON_FEATURE_COLUMNS
        and pd.api.types.is_numeric_dtype(merged[column])
    ]

    if not feature_columns or len(merged) < 100:
        return None

    matrix = merged[feature_columns].astype(float).fillna(0.0)
    target = merged[target_column].astype(float)

    split = int(len(matrix) * 0.8)
    train_x, holdout_x = matrix.iloc[:split], matrix.iloc[split:]
    train_y, holdout_y = target.iloc[:split], target.iloc[split:]

    surrogate = lgb.LGBMRegressor(
        n_estimators=250,
        learning_rate=0.06,
        max_depth=7,
        num_leaves=48,
        subsample=0.85,
        colsample_bytree=0.85,
        random_state=42,
        verbose=-1,
    )
    surrogate.fit(train_x, train_y)

    predictions = surrogate.predict(holdout_x)
    residual_sum = float(np.sum((holdout_y.values - predictions) ** 2))
    total_sum = float(np.sum((holdout_y.values - holdout_y.mean()) ** 2))
    fidelity = 1.0 - safe_ratio(residual_sum, total_sum) if total_sum > 0 else 0.0

    explainer = shap.TreeExplainer(surrogate)
    explanation_frame = holdout_x.reset_index(drop=True)
    shap_values = explainer.shap_values(explanation_frame)

    base_value = explainer.expected_value
    if isinstance(base_value, (list, np.ndarray)):
        base_value = float(np.ravel(base_value)[0])

    return SurrogateExplanation(
        feature_names=feature_columns,
        shap_values=np.asarray(shap_values),
        feature_frame=explanation_frame,
        base_value=float(base_value),
        fidelity_r2=float(fidelity),
        explained_target=explained_target,
        row_count=len(explanation_frame),
    )


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------


def render_overview(
    zones: pd.DataFrame, windows: pd.DataFrame, bounds: Dict[str, Any], settings: Settings
) -> None:
    st.subheader("System state")

    if windows.empty:
        st.info("No demand windows in the selected range. Check that the streaming stage is running.")
        return

    latest_slot = windows["window_start"].max()
    current = windows[windows["window_start"] == latest_slot]

    columns = st.columns(5)
    columns[0].metric("Active zones", f"{current['location_id'].nunique():,}")
    columns[1].metric("Trips in latest window", f"{int(current['trip_count'].sum()):,}")
    columns[2].metric("Revenue in latest window", f"${current['total_revenue'].sum():,.0f}")
    columns[3].metric("Zones in surge", f"{int(current['is_surge'].sum()):,}")
    columns[4].metric("Latest window", format_window(int(latest_slot), settings.display_timezone))

    st.markdown("#### City-wide demand")

    citywide = (
        windows.groupby("window_start", as_index=False)
        .agg(
            trip_count=("trip_count", "sum"),
            surge_zones=("is_surge", "sum"),
            revenue=("total_revenue", "sum"),
        )
        .sort_values("window_start")
    )
    citywide["window_time"] = to_local_datetime(citywide["window_start"], settings.display_timezone)

    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=citywide["window_time"],
            y=citywide["trip_count"],
            name="Trips",
            mode="lines",
            line=dict(width=2),
        )
    )
    figure.add_trace(
        go.Bar(
            x=citywide["window_time"],
            y=citywide["surge_zones"],
            name="Surging zones",
            opacity=0.35,
            yaxis="y2",
        )
    )
    figure.update_layout(
        height=340,
        margin=dict(l=10, r=10, t=30, b=10),
        yaxis=dict(title="Trips per window"),
        yaxis2=dict(title="Surging zones", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", y=1.12),
    )
    st.plotly_chart(figure, use_container_width=True)

    left, right = st.columns(2)

    with left:
        st.markdown("#### Busiest zones, latest window")
        busiest = (
            current.merge(zones[["location_id", "zone_name", "borough"]], on="location_id", how="left")
            .nlargest(12, "trip_count")[["zone_name", "borough", "trip_count", "demand_per_minute"]]
            .rename(
                columns={
                    "zone_name": "Zone",
                    "borough": "Borough",
                    "trip_count": "Trips",
                    "demand_per_minute": "Trips/min",
                }
            )
        )
        st.dataframe(busiest, use_container_width=True, hide_index=True)

    with right:
        st.markdown("#### Borough share")
        by_borough = (
            current.merge(zones[["location_id", "borough"]], on="location_id", how="left")
            .groupby("borough", as_index=False)["trip_count"]
            .sum()
            .sort_values("trip_count", ascending=False)
        )
        by_borough = by_borough[by_borough["borough"] != ""]
        if by_borough.empty:
            st.info("Borough metadata has not been loaded onto the zone dimension.")
        else:
            st.plotly_chart(
                px.bar(by_borough, x="borough", y="trip_count", labels={"trip_count": "Trips", "borough": ""}),
                use_container_width=True,
            )


def render_demand_map(
    zones: pd.DataFrame,
    windows: pd.DataFrame,
    forecasts: pd.DataFrame,
    geojson: Optional[Dict[str, Any]],
    settings: Settings,
) -> None:
    st.subheader("Demand and forecast map")

    if windows.empty:
        st.info("No demand windows to map.")
        return

    slots = sorted(windows["window_start"].unique())
    labels = [format_window(int(slot), settings.display_timezone) for slot in slots]

    selected_index = st.select_slider(
        "Window",
        options=list(range(len(slots))),
        value=len(slots) - 1,
        format_func=lambda index: labels[index],
    )
    selected_slot = int(slots[selected_index])

    snapshot = windows[windows["window_start"] == selected_slot].merge(
        zones[["location_id", "zone_name", "borough", "latitude", "longitude", "importance", "community_id"]],
        on="location_id",
        how="left",
    )
    snapshot = snapshot.dropna(subset=["latitude", "longitude"])

    if not forecasts.empty:
        forecast_slice = forecasts[forecasts["origin_window_start"] == selected_slot][
            ["location_id", "forecast_trip_count", "lower_bound", "upper_bound"]
        ]
        snapshot = snapshot.merge(forecast_slice, on="location_id", how="left")
        snapshot["forecast_trip_count"] = snapshot["forecast_trip_count"].fillna(0.0)
        snapshot["forecast_delta"] = snapshot["forecast_trip_count"] - snapshot["trip_count"]
    else:
        snapshot["forecast_trip_count"] = 0.0
        snapshot["forecast_delta"] = 0.0

    datasets: Dict[str, Any] = {
        "zone_demand": snapshot[
            [
                "location_id",
                "zone_name",
                "borough",
                "latitude",
                "longitude",
                "trip_count",
                "demand_per_minute",
                "total_revenue",
                "surge_ratio",
                "is_surge",
                "importance",
                "community_id",
                "forecast_trip_count",
                "forecast_delta",
            ]
        ]
    }

    if geojson is not None:
        datasets["zone_polygons"] = enrich_geojson(
            geojson,
            snapshot,
            ["trip_count", "demand_per_minute", "forecast_trip_count", "forecast_delta", "importance"],
        )

    render_kepler(datasets)

    st.caption(
        "Point size and colour are auto-derived by Kepler from the supplied columns; "
        "use the layer panel to switch between observed trips, forecast, and the forecast delta."
    )

    if not forecasts.empty:
        st.markdown("#### Largest forecast movements")
        movements = snapshot.nlargest(10, "forecast_delta")[
            ["zone_name", "borough", "trip_count", "forecast_trip_count", "forecast_delta"]
        ].rename(
            columns={
                "zone_name": "Zone",
                "borough": "Borough",
                "trip_count": "Observed",
                "forecast_trip_count": "Forecast",
                "forecast_delta": "Change",
            }
        )
        st.dataframe(movements, use_container_width=True, hide_index=True)


def render_graph_structure(
    zones: pd.DataFrame, flows: pd.DataFrame, communities: pd.DataFrame
) -> None:
    st.subheader("Graph structure")

    if zones.empty:
        st.info("The zone dimension is empty.")
        return

    st.markdown("#### Zone importance")
    st.caption(
        "PageRank captures sustained demand pull. Betweenness captures corridor position — "
        "zones that carry traffic between others without generating it. A positive corridor bias "
        "marks a zone whose disruption propagates further than its own trip volume implies."
    )

    left, right = st.columns(2)

    with left:
        ranked = zones.nlargest(15, "importance")[
            ["zone_name", "borough", "importance", "page_rank", "betweenness", "corridor_bias"]
        ].rename(
            columns={
                "zone_name": "Zone",
                "borough": "Borough",
                "importance": "Importance",
                "page_rank": "PageRank",
                "betweenness": "Betweenness",
                "corridor_bias": "Corridor bias",
            }
        )
        st.dataframe(ranked, use_container_width=True, hide_index=True)

    with right:
        scatter = zones[zones["importance"] > 0]
        if scatter.empty:
            st.info("Centrality has not been computed yet. Run the graph refresh cycle.")
        else:
            st.plotly_chart(
                px.scatter(
                    scatter,
                    x="page_rank",
                    y="betweenness",
                    size="latest_trip_count",
                    color="borough",
                    hover_name="zone_name",
                    labels={"page_rank": "PageRank", "betweenness": "Betweenness"},
                ),
                use_container_width=True,
            )

    st.markdown("#### Dominant flows")
    if flows.empty:
        st.info("No flow edges match the selected range and threshold.")
    else:
        arcs = flows.dropna(subset=["origin_lat", "origin_lng", "destination_lat", "destination_lng"])
        if arcs.empty:
            st.info("Flow endpoints have no centroids; load the zone dimension geometry.")
        else:
            render_kepler({"zone_flows": arcs}, height=560)

        st.dataframe(
            flows.head(15)[
                ["origin_name", "destination_name", "trips", "avg_duration_seconds", "avg_distance_miles"]
            ].rename(
                columns={
                    "origin_name": "Origin",
                    "destination_name": "Destination",
                    "trips": "Trips",
                    "avg_duration_seconds": "Avg duration (s)",
                    "avg_distance_miles": "Avg distance (mi)",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("#### Detected communities")
    if communities.empty:
        st.info("Community detection has not run yet.")
    else:
        st.caption(
            "Cohesion is the share of a community's trips that stay inside it. "
            "Communities spanning several boroughs are the ones worth attention: "
            "if the partition merely reproduced borough boundaries it would add nothing."
        )
        display = communities.copy()
        display["crosses_boroughs"] = display["borough_spread"] > 1
        st.dataframe(
            display[
                ["community_id", "size", "cohesion", "dominant_borough", "borough_spread", "crosses_boroughs"]
            ].rename(
                columns={
                    "community_id": "Community",
                    "size": "Zones",
                    "cohesion": "Cohesion",
                    "dominant_borough": "Dominant borough",
                    "borough_spread": "Boroughs",
                    "crosses_boroughs": "Cross-borough",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )


def render_forecast_explorer(
    zones: pd.DataFrame,
    forecasts: pd.DataFrame,
    settings: Settings,
    since: int,
) -> None:
    st.subheader("Forecast explorer")

    if zones.empty:
        st.info("The zone dimension is empty.")
        return

    options = zones.sort_values("importance", ascending=False)
    labels = {
        int(row.location_id): f"{row.zone_name or 'Zone'} ({row.borough or 'unknown'})"
        for row in options.itertuples()
    }

    selected_zone = st.selectbox(
        "Zone",
        options=list(labels.keys()),
        format_func=lambda value: labels.get(value, str(value)),
    )

    history = load_zone_history(
        settings.neo4j_uri,
        settings.neo4j_user,
        settings.neo4j_password,
        settings.neo4j_database,
        int(selected_zone),
        since,
    )

    if history.empty:
        st.info("No demand history for this zone in the selected range.")
        return

    history["window_time"] = to_local_datetime(history["window_start"], settings.display_timezone)

    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=history["window_time"],
            y=history["trip_count"],
            name="Observed trips",
            mode="lines",
            line=dict(width=2),
        )
    )

    zone_forecasts = pd.DataFrame()
    if not forecasts.empty:
        zone_forecasts = forecasts[forecasts["location_id"] == selected_zone].copy()

    if not zone_forecasts.empty:
        zone_forecasts["target_time"] = to_local_datetime(
            zone_forecasts["target_window_start"], settings.display_timezone
        )
        zone_forecasts = zone_forecasts.sort_values("target_time")

        figure.add_trace(
            go.Scatter(
                x=zone_forecasts["target_time"],
                y=zone_forecasts["upper_bound"],
                name="Upper bound",
                mode="lines",
                line=dict(width=0),
                showlegend=False,
            )
        )
        figure.add_trace(
            go.Scatter(
                x=zone_forecasts["target_time"],
                y=zone_forecasts["lower_bound"],
                name="Prediction interval",
                mode="lines",
                line=dict(width=0),
                fill="tonexty",
                fillcolor="rgba(120, 160, 255, 0.25)",
            )
        )
        figure.add_trace(
            go.Scatter(
                x=zone_forecasts["target_time"],
                y=zone_forecasts["forecast_trip_count"],
                name="Forecast",
                mode="lines+markers",
                line=dict(width=2, dash="dash"),
            )
        )

    figure.update_layout(
        height=420,
        margin=dict(l=10, r=10, t=30, b=10),
        yaxis=dict(title="Trips per window"),
        legend=dict(orientation="h", y=1.12),
    )
    st.plotly_chart(figure, use_container_width=True)

    columns = st.columns(4)
    columns[0].metric("Mean trips", f"{history['trip_count'].mean():,.1f}")
    columns[1].metric("Peak trips", f"{int(history['trip_count'].max()):,}")
    columns[2].metric("Surging windows", f"{int(history['is_surge'].sum()):,}")

    if zone_forecasts.empty:
        columns[3].metric("Model version", "no forecast")
    else:
        versions = zone_forecasts["model_version"].dropna().unique()
        columns[3].metric("Model version", versions[0] if len(versions) else "unknown")

    st.markdown("#### Surge history")
    surge = history[history["is_surge"]]
    if surge.empty:
        st.caption("This zone has not surged in the selected range.")
    else:
        st.plotly_chart(
            px.scatter(
                surge,
                x="window_time",
                y="surge_ratio",
                size="trip_count",
                labels={"window_time": "", "surge_ratio": "Surge ratio"},
            ),
            use_container_width=True,
        )


def render_explainability(zones: pd.DataFrame, settings: Settings) -> None:
    st.subheader("Feature attribution")

    st.caption(
        "The deployed forecaster is a Spark MLlib ensemble that cannot be loaded into this "
        "process. A gradient boosted surrogate is fitted against the deployed model's own "
        "predictions, and SHAP values are computed over that surrogate. The fidelity score "
        "below is how much of the deployed model's variance the surrogate reproduces — "
        "attributions are only as trustworthy as that number."
    )

    explanation = fit_surrogate(
        settings.feature_store_path, settings.forecast_path, settings.surrogate_sample_rows
    )

    if explanation is None:
        st.info(
            "No feature store available. Run the training cycle to populate "
            f"{settings.feature_store_path} before using this view."
        )
        return

    columns = st.columns(3)
    columns[0].metric("Surrogate fidelity (R²)", f"{explanation.fidelity_r2:.3f}")
    columns[1].metric("Explained rows", f"{explanation.row_count:,}")
    columns[2].metric("Features", f"{len(explanation.feature_names):,}")

    if explanation.explained_target != "deployed model predictions":
        st.warning(
            "No forecast table was found, so the surrogate was fitted against observed demand. "
            "This explains the phenomenon, not the deployed model."
        )

    if not explanation.is_faithful:
        st.warning(
            f"Surrogate fidelity is {explanation.fidelity_r2:.3f}. The surrogate does not track "
            "the deployed model closely enough for these attributions to be relied on; treat "
            "them as directional only."
        )

    st.markdown("#### Global importance")

    top_n = st.slider("Features shown", min_value=5, max_value=40, value=18, step=1)
    importance = explanation.global_importance().head(top_n)

    st.plotly_chart(
        px.bar(
            importance.sort_values("mean_abs_shap"),
            x="mean_abs_shap",
            y="feature",
            orientation="h",
            labels={"mean_abs_shap": "Mean |SHAP| (trips)", "feature": ""},
            height=max(320, 22 * len(importance)),
        ),
        use_container_width=True,
    )

    graph_features = {
        "page_rank_score",
        "betweenness_score",
        "importance_score",
        "importance_rank",
        "corridor_bias",
        "community_id",
        "community_size",
        "community_cohesion",
        "community_peer_mean_demand",
        "community_demand_share",
        "inbound_trip_count",
        "inbound_zone_count",
        "outbound_trip_count",
        "outbound_zone_count",
        "net_flow",
    }

    full_importance = explanation.global_importance()
    graph_share = safe_ratio(
        full_importance[full_importance["feature"].isin(graph_features)]["mean_abs_shap"].sum(),
        full_importance["mean_abs_shap"].sum(),
    )

    st.metric(
        "Attribution carried by graph-derived features",
        f"{graph_share:.1%}",
        help=(
            "Share of total attribution magnitude coming from centrality, community and flow "
            "features. A low share means the graph layer is not earning its cost."
        ),
    )

    unused = full_importance[full_importance["mean_abs_shap"] <= 0.0]["feature"].tolist()
    if unused:
        st.warning(
            "Features contributing nothing, usually a broken join upstream: "
            + ", ".join(unused[:15])
            + ("…" if len(unused) > 15 else "")
        )

    st.markdown("#### Single prediction breakdown")

    row_index = st.number_input(
        "Explained row",
        min_value=0,
        max_value=explanation.row_count - 1,
        value=0,
        step=1,
        help="Index into the held-out sample the surrogate was evaluated on.",
    )

    contributions = explanation.local_contributions(int(row_index))
    prediction = explanation.base_value + explanation.shap_values[int(row_index)].sum()

    detail = st.columns(2)
    detail[0].metric("Baseline (mean prediction)", f"{explanation.base_value:,.2f}")
    detail[1].metric("This prediction", f"{prediction:,.2f}")

    waterfall = go.Figure(
        go.Waterfall(
            orientation="h",
            y=contributions["feature"][::-1],
            x=contributions["shap_value"][::-1],
            connector=dict(line=dict(width=1)),
            decreasing=dict(marker=dict(color="#d9534f")),
            increasing=dict(marker=dict(color="#5cb85c")),
        )
    )
    waterfall.update_layout(
        height=max(360, 26 * len(contributions)),
        margin=dict(l=10, r=10, t=30, b=10),
        xaxis=dict(title="Contribution to forecast (trips)"),
    )
    st.plotly_chart(waterfall, use_container_width=True)

    st.dataframe(
        contributions[["feature", "feature_value", "shap_value"]].rename(
            columns={
                "feature": "Feature",
                "feature_value": "Value",
                "shap_value": "Contribution",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    if not zones.empty:
        st.caption(
            f"Sample drawn from {settings.feature_store_path}. Row indices refer to the "
            "surrogate's held-out split, not to a specific zone and window."
        )


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------


def render_sidebar(settings: Settings, bounds: Dict[str, Any]) -> Tuple[int, int, int]:
    st.sidebar.title("NYC Taxi Demand")
    st.sidebar.caption("Real-time graph analytics and demand forecasting")

    if bounds["latest"] is None:
        st.sidebar.warning("No demand windows found.")
    else:
        st.sidebar.metric(
            "Latest window", format_window(bounds["latest"], settings.display_timezone)
        )
        st.sidebar.caption(f"{bounds['count']:,} windows retained")

    lookback_hours = st.sidebar.slider("Lookback (hours)", min_value=1, max_value=72, value=6)
    min_flow_trips = st.sidebar.slider("Minimum trips per flow", min_value=1, max_value=200, value=20)
    flow_limit = st.sidebar.slider("Flow edges shown", min_value=50, max_value=1000, value=300, step=50)

    if st.sidebar.button("Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.divider()
    st.sidebar.caption(f"Graph: {settings.neo4j_uri}")
    st.sidebar.caption(f"Features: {settings.feature_store_path}")
    st.sidebar.caption(f"Forecasts: {settings.forecast_path}")

    return lookback_hours, min_flow_trips, flow_limit


def main() -> None:
    st.set_page_config(
        page_title="NYC Taxi Demand Analytics",
        page_icon="🗺️",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    settings = load_settings()

    if not settings.has_password:
        st.error("NEO4J_PASSWORD is not set. Export it before starting the dashboard.")
        st.stop()

    try:
        bounds = load_window_bounds(
            settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password, settings.neo4j_database
        )
    except Exception as error:  # noqa: BLE001 - surfaced to the operator verbatim
        st.error(f"Could not reach the graph at {settings.neo4j_uri}: {error}")
        st.stop()

    lookback_hours, min_flow_trips, flow_limit = render_sidebar(settings, bounds)

    if bounds["latest"] is None:
        st.warning(
            "The graph holds no demand windows. Start the streaming stage and the trip producer, "
            "then refresh."
        )
        st.stop()

    since = int(bounds["latest"]) - (lookback_hours * MILLIS_PER_HOUR)

    zones = load_zones(
        settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password, settings.neo4j_database
    )
    windows = load_demand_windows(
        settings.neo4j_uri,
        settings.neo4j_user,
        settings.neo4j_password,
        settings.neo4j_database,
        since,
    )
    flows = load_flows(
        settings.neo4j_uri,
        settings.neo4j_user,
        settings.neo4j_password,
        settings.neo4j_database,
        since,
        min_flow_trips,
        flow_limit,
    )
    communities = load_communities(
        settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password, settings.neo4j_database
    )
    forecasts = load_forecasts_from_graph(
        settings.neo4j_uri,
        settings.neo4j_user,
        settings.neo4j_password,
        settings.neo4j_database,
        since,
    )
    geojson = load_zone_geojson(settings.zone_geojson_path)

    if geojson is None:
        st.sidebar.caption("Zone polygons not provisioned; maps fall back to centroids.")

    overview_tab, map_tab, graph_tab, forecast_tab, explain_tab = st.tabs(
        ["Overview", "Demand map", "Graph structure", "Forecast explorer", "Explainability"]
    )

    with overview_tab:
        render_overview(zones, windows, bounds, settings)

    with map_tab:
        render_demand_map(zones, windows, forecasts, geojson, settings)

    with graph_tab:
        render_graph_structure(zones, flows, communities)

    with forecast_tab:
        render_forecast_explorer(zones, forecasts, settings, since)

    with explain_tab:
        render_explainability(zones, settings)


if __name__ == "__main__":
    main()
