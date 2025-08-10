// ---------------------------------------------------------------------------
// SBT plugin definitions
// ---------------------------------------------------------------------------

// Fat-jar packaging for spark-submit deployment
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.5")

// Source formatting (scalafmt) and import ordering
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

// Static analysis
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.11.1")

// Test coverage reporting
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.11")

// Dependency tree inspection and eviction diagnostics
addSbtPlugin("com.github.sbt" % "sbt-dependency-tree" % "1.0.0")

// Vulnerability scanning against the OSS Index / OWASP feeds
addSbtPlugin("net.vonbuchholtz" % "sbt-dependency-check" % "5.1.0")

// Docker / systemd / archive packaging for the batch and streaming images
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.16")

// Build info accessor for runtime version reporting
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.11.0")

// Ensures deterministic, reproducible artifact output
addSbtPlugin("io.github.er1c" % "sbt-reproducible-builds" % "0.1.0")
