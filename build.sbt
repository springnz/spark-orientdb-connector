lazy val commonSettings = Seq(
  organization := "com.metreta",
  version := "brench-master",
  scalaVersion := "2.11.4",
  libraryDependencies ++= Seq(
    "com.orientechnologies" % "orientdb-core" % "2.0.15",
    "com.orientechnologies" % "orientdb-client" % "2.0.15",
    "com.orientechnologies" % "orientdb-jdbc" % "2.0.15",
    "com.orientechnologies" % "orientdb-graphdb" % "2.0.15",
    "com.orientechnologies" % "orientdb-distributed" % "2.0.15",
    "org.scalatest" % "scalatest_2.11" % "2.2.4",
    "org.apache.spark" % "spark-core_2.11" % "1.4.0",
    "org.apache.spark" % "spark-graphx_2.11" % "1.4.0",
    "org.apache.spark" % "spark-unsafe_2.11" % "1.4.0",
    "org.apache.spark" % "spark-network-common_2.11" % "1.4.0",
    "org.apache.spark" % "spark-network-shuffle_2.11" % "1.4.0",
    "org.apache.spark" % "spark-launcher_2.11" % "1.4.0",
    "com.tinkerpop.blueprints" % "blueprints-core" % "2.6.0"
    ),
    externalResolvers := Seq(DefaultMavenRepository),
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
