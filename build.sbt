val sparkVersion = "1.6.2"
val orientVersion = "2.1.5"
val repo = "https://nexus.prod.corp/content"

val crossScala = Seq("2.10.5", "2.11.8")

/* Leverages optional Spark 'scala-2.10' profile optionally set by the user via -Dscala-2.10=true if enabled */
lazy val scalaVer = sys.props.get("scala-2.10") match {
  case Some(is) if is.nonEmpty && is.toBoolean => crossScala.head
  case crossBuildFor                           => crossScala.last
}

lazy val commonSettings = Seq(
  /* Spring specific Stuff */
  organization := "springnz",
  publishTo := {
    if (isSnapshot.value)
      Some("snapshots" at s"$repo/repositories/snapshots")
    else
      Some("releases" at s"$repo/repositories/releases")
  },
  /* End Spring specific Stuff */
  scalaVersion := scalaVer,
  crossScalaVersions := crossScala,
  crossVersion := CrossVersion.binary,
  fork:= true,
  libraryDependencies ++= Seq(
    "com.orientechnologies" % "orientdb-client" % orientVersion,
    "com.orientechnologies" % "orientdb-graphdb" % orientVersion,
    "org.scalatest" %% "scalatest" % "2.2.4",
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-graphx" % sparkVersion % Provided,
    "com.tinkerpop.blueprints" % "blueprints-core" % "2.6.0"
    ),
    externalResolvers ++= Seq(DefaultMavenRepository),
    parallelExecution in Test := false
)

lazy val connector = (project in file("./spark-orientdb-connector")).
  settings(commonSettings: _*).
  settings(
    name := "spark-orientdb-connector"
    )

lazy val demos = (project in file("./spark-orientdb-connector-demos")).
  settings(commonSettings: _*).
  settings(
    name := "spark-orientdb-connector-demos"
    ).
   dependsOn(connector)

lazy val main = (project in file("."))
  .aggregate(connector, demos)
  .settings(Defaults.coreDefaultSettings ++ Seq(
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),
    publishArtifact := false
  ))
  .settings(parallelExecution in Test := false)

