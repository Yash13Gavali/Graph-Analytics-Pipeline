import sbtassembly.MergeStrategy

// ---------------------------------------------------------------------------
// Real-Time Graph Analytics & NYC Taxi Demand Forecasting
// Scala / Spark build definition
// ---------------------------------------------------------------------------

ThisBuild / organization := "com.nyctaxi.graph"
ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

// --- Pinned dependency versions --------------------------------------------
val sparkVersion            = "3.5.1"
val hadoopVersion           = "3.3.6"
val sedonaVersion           = "1.5.1"
val geotoolsWrapperVersion  = "1.5.1-28.2"
val graphFramesVersion      = "0.8.3-spark3.5-s_2.12"
val neo4jConnectorVersion   = "5.3.0_for_spark_3"
val jtsVersion              = "1.19.0"
val h3Version               = "4.1.1"
val kafkaClientsVersion     = "3.6.1"
val jacksonVersion          = "2.15.2"
val circeVersion            = "0.14.6"
val pureConfigVersion       = "0.17.6"
val catsEffectVersion       = "3.5.4"
val logbackVersion          = "1.4.14"
val scalaLoggingVersion     = "3.9.5"
val scalatestVersion        = "3.2.18"
val scalacheckVersion       = "3.2.18.0"
val sparkFastTestsVersion   = "1.5.0"
val testcontainersVersion   = "0.41.3"

// --- Resolvers ---------------------------------------------------------------
ThisBuild / resolvers ++= Seq(
  "Spark Packages Repo"   at "https://repos.spark-packages.org",
  "OSGeo Release"         at "https://repo.osgeo.org/repository/release/",
  "Confluent"             at "https://packages.confluent.io/maven/",
  "Neo4j Public"          at "https://repo.gradle.org/artifactory/libs-release/",
  Resolver.mavenLocal
)

// --- Compiler / JVM options --------------------------------------------------
ThisBuild / scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-explaintypes",
  "-target:jvm-1.8",
  "-Xlint:_",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused:imports,privates,locals",
  "-Xfatal-warnings"
)

ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

ThisBuild / javaOptions ++= Seq(
  "-Xmx4G",
  "-XX:+UseG1GC",
  "-Dio.netty.tryReflectionSetAccessible=true",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
)

// --- Dependency groups -------------------------------------------------------
// Spark is `provided`: the cluster supplies it, the fat jar must not shade it.
val sparkCore: Seq[ModuleID] = Seq(
  "org.apache.spark" %% "spark-core"      % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql"       % sparkVersion % Provided,
  "org.apache.spark" %% "spark-catalyst"  % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib"     % sparkVersion % Provided,
  "org.apache.spark" %% "spark-graphx"    % sparkVersion % Provided,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-hive"      % sparkVersion % Provided
)

val sparkStreamingConnectors: Seq[ModuleID] = Seq(
  "org.apache.spark" %% "spark-sql-kafka-0-10"       % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-token-provider-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-avro"                 % sparkVersion,
  "org.apache.kafka"  % "kafka-clients"              % kafkaClientsVersion
)

val graphLibraries: Seq[ModuleID] = Seq(
  "graphframes" % "graphframes" % graphFramesVersion,
  "org.neo4j"   % "neo4j-connector-apache-spark_2.12" % neo4jConnectorVersion
)

// Sedona shaded artifact bundles the Spark-side spatial SQL/RDD API;
// geotools-wrapper supplies the CRS/EPSG database for reprojection.
val spatialLibraries: Seq[ModuleID] = Seq(
  "org.apache.sedona" %% "sedona-spark-shaded-3.5"  % sedonaVersion,
  "org.datasyslab"     % "geotools-wrapper"         % geotoolsWrapperVersion,
  "org.locationtech.jts" % "jts-core"               % jtsVersion,
  "com.uber"           % "h3"                       % h3Version
)

val serialization: Seq[ModuleID] = Seq(
  "com.fasterxml.jackson.core"       % "jackson-databind"      % jacksonVersion,
  "com.fasterxml.jackson.module"    %% "jackson-module-scala"   % jacksonVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion,
  "io.circe" %% "circe-core"    % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser"  % circeVersion
)

val runtimeSupport: Seq[ModuleID] = Seq(
  "com.github.pureconfig" %% "pureconfig"          % pureConfigVersion,
  "org.typelevel"         %% "cats-effect"         % catsEffectVersion,
  "com.typesafe.scala-logging" %% "scala-logging"  % scalaLoggingVersion,
  "ch.qos.logback"         % "logback-classic"     % logbackVersion,
  "org.apache.hadoop"      % "hadoop-aws"          % hadoopVersion % Provided,
  "org.apache.hadoop"      % "hadoop-client-api"   % hadoopVersion % Provided
)

val testLibraries: Seq[ModuleID] = Seq(
  "org.scalatest"     %% "scalatest"                        % scalatestVersion   % Test,
  "org.scalatestplus" %% "scalacheck-1-17"                  % scalacheckVersion  % Test,
  "com.github.mrpowers" %% "spark-fast-tests"               % sparkFastTestsVersion % Test,
  "com.dimafeng"      %% "testcontainers-scala-scalatest"   % testcontainersVersion % Test,
  "com.dimafeng"      %% "testcontainers-scala-kafka"       % testcontainersVersion % Test,
  "com.dimafeng"      %% "testcontainers-scala-neo4j"       % testcontainersVersion % Test,
  "org.apache.spark"  %% "spark-core"  % sparkVersion % Test classifier "tests",
  "org.apache.spark"  %% "spark-sql"   % sparkVersion % Test classifier "tests",
  "org.apache.spark"  %% "spark-catalyst" % sparkVersion % Test classifier "tests"
)

// --- Dependency conflict management -----------------------------------------
ThisBuild / excludeDependencies ++= Seq(
  ExclusionRule("org.slf4j", "slf4j-log4j12"),
  ExclusionRule("log4j", "log4j"),
  ExclusionRule("commons-logging", "commons-logging")
)

ThisBuild / dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core"    % "jackson-databind"     % jacksonVersion,
  "com.fasterxml.jackson.core"    % "jackson-core"         % jacksonVersion,
  "com.fasterxml.jackson.core"    % "jackson-annotations"  % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
)

// --- Assembly ----------------------------------------------------------------
lazy val assemblyMergeSettings = Seq(
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "services", _ @_*)              => MergeStrategy.filterDistinctLines
    case PathList("META-INF", xs @ _*) if xs.lastOption.exists(_.endsWith(".SF"))  => MergeStrategy.discard
    case PathList("META-INF", xs @ _*) if xs.lastOption.exists(_.endsWith(".DSA")) => MergeStrategy.discard
    case PathList("META-INF", xs @ _*) if xs.lastOption.exists(_.endsWith(".RSA")) => MergeStrategy.discard
    case PathList("META-INF", "MANIFEST.MF")                  => MergeStrategy.discard
    case PathList("META-INF", "versions", _ @_*)              => MergeStrategy.first
    case PathList("module-info.class")                        => MergeStrategy.discard
    case PathList("git.properties")                           => MergeStrategy.discard
    case "reference.conf"                                     => MergeStrategy.concat
    case "application.conf"                                   => MergeStrategy.concat
    case "logback.xml"                                        => MergeStrategy.first
    case "log4j2.properties"                                  => MergeStrategy.first
    case PathList("org", "locationtech", _ @_*)               => MergeStrategy.first
    case PathList("org", "geotools", _ @_*)                   => MergeStrategy.first
    case PathList("com", "fasterxml", "jackson", _ @_*)       => MergeStrategy.first
    case other                                                => (assembly / assemblyMergeStrategy).value(other)
  },
  assembly / assemblyShadeRules := Seq(
    ShadeRule
      .rename("com.google.common.**" -> "shaded.guava.@1")
      .inLibrary("com.google.guava" % "guava" % "32.1.3-jre")
      .inAll
  ),
  assembly / test := {},
  assembly / assemblyOption ~= { _.withIncludeScala(false) }
)

// --- Test execution ----------------------------------------------------------
lazy val testSettings = Seq(
  Test / fork              := true,
  Test / parallelExecution := false,
  Test / testOptions       += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  Test / envVars           := Map("SPARK_LOCAL_IP" -> "127.0.0.1")
)

// --- Modules -----------------------------------------------------------------
lazy val commonSettings = testSettings ++ assemblyMergeSettings ++ Seq(
  Compile / doc / sources := Seq.empty,
  Compile / packageDoc / publishArtifact := false,
  autoAPIMappings := true
)

lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name := "taxi-graph-core",
    libraryDependencies ++= sparkCore ++ serialization ++ runtimeSupport ++ testLibraries
  )

lazy val ingestion = (project in file("ingestion"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    name := "taxi-graph-ingestion",
    libraryDependencies ++= sparkCore ++ sparkStreamingConnectors ++ testLibraries,
    assembly / assemblyJarName := s"taxi-graph-ingestion-${version.value}.jar",
    assembly / mainClass := Some("com.nyctaxi.graph.ingestion.StreamingIngestionJob")
  )

lazy val spatial = (project in file("spatial"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    name := "taxi-graph-spatial",
    libraryDependencies ++= sparkCore ++ spatialLibraries ++ testLibraries
  )

lazy val graph = (project in file("graph"))
  .dependsOn(core % "compile->compile;test->test", spatial)
  .settings(commonSettings)
  .settings(
    name := "taxi-graph-analytics",
    libraryDependencies ++= sparkCore ++ graphLibraries ++ spatialLibraries ++ testLibraries,
    assembly / assemblyJarName := s"taxi-graph-analytics-${version.value}.jar",
    assembly / mainClass := Some("com.nyctaxi.graph.analytics.GraphAnalyticsJob")
  )

lazy val forecasting = (project in file("forecasting"))
  .dependsOn(core % "compile->compile;test->test", spatial, graph)
  .settings(commonSettings)
  .settings(
    name := "taxi-graph-forecasting",
    libraryDependencies ++= sparkCore ++ spatialLibraries ++ testLibraries,
    assembly / assemblyJarName := s"taxi-graph-forecasting-${version.value}.jar",
    assembly / mainClass := Some("com.nyctaxi.graph.forecasting.DemandForecastJob")
  )

lazy val root = (project in file("."))
  .aggregate(core, ingestion, spatial, graph, forecasting)
  .settings(commonSettings)
  .settings(
    name := "nyc-taxi-graph-analytics",
    publish / skip := true
  )
