val sparkVersion = "1.5.1"
val orientVersion = "2.1.5"

lazy val commonSettings = Seq(
  organization := "com.metreta",
  version := "1.0",
  scalaVersion := "2.11.7",
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
